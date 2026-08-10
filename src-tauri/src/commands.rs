#![allow(
    clippy::needless_pass_by_value,
    reason = "Tauri command arguments are extracted by value by the IPC macro"
)]

use std::fmt::Write as _;
use std::path::PathBuf;

use roadie_core::{WorkerEngine, WorkerHost, build_probe_script, ssh_args};
use serde::Serialize;
use tauri::State;
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::coordinator::CoordinatorCommand;
use crate::models::{AppSettings, Bootstrap, Shared, read_shared, write_shared};
use crate::persistence::save_settings;
use crate::process_env::configure_local_worker_command;

pub struct AppState {
    pub shared: Shared,
    pub commands: mpsc::UnboundedSender<CoordinatorCommand>,
    pub settings_path: PathBuf,
}

#[tauri::command]
pub fn bootstrap(state: State<'_, AppState>) -> Bootstrap {
    let shared = read_shared(&state.shared);
    Bootstrap {
        settings: shared.settings.clone(),
        view: shared.view.clone(),
    }
}

#[tauri::command]
pub fn save_app_settings(settings: AppSettings, state: State<'_, AppState>) -> Result<(), String> {
    settings.validate()?;
    save_settings(&state.settings_path, &settings)?;
    write_shared(&state.shared).settings.clone_from(&settings);
    state
        .commands
        .send(CoordinatorCommand::SettingsUpdated(Box::new(settings)))
        .map_err(|_| "voice coordinator is unavailable".to_owned())
}

#[tauri::command]
pub fn send_text_prompt(prompt: String, state: State<'_, AppState>) -> Result<(), String> {
    send(&state, CoordinatorCommand::SubmitPrompt(prompt))
}

#[tauri::command]
pub fn start_listening(state: State<'_, AppState>) -> Result<(), String> {
    send(&state, CoordinatorCommand::StartListening)
}

#[tauri::command]
pub fn stop_listening(state: State<'_, AppState>) -> Result<(), String> {
    send(&state, CoordinatorCommand::StopListening)
}

#[tauri::command]
pub fn new_session(state: State<'_, AppState>) -> Result<(), String> {
    send(&state, CoordinatorCommand::NewSession)
}

#[tauri::command]
pub fn cancel_turn(state: State<'_, AppState>) -> Result<(), String> {
    send(&state, CoordinatorCommand::CancelTurn)
}

#[tauri::command]
pub fn stop_speaking(state: State<'_, AppState>) -> Result<(), String> {
    send(&state, CoordinatorCommand::StopSpeaking)
}

#[tauri::command]
pub fn repeat_last(state: State<'_, AppState>) -> Result<(), String> {
    send(&state, CoordinatorCommand::RepeatLast)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeResult {
    pub success: bool,
    pub output: String,
}

#[tauri::command]
pub async fn test_connection(state: State<'_, AppState>) -> Result<ProbeResult, String> {
    let settings = read_shared(&state.shared).settings.clone();
    let config = settings.remote_config();
    config.validate().map_err(|error| error.to_string())?;
    if config.host == WorkerHost::LocalMac {
        let workspace_ok = std::path::Path::new(&config.workspace).is_dir();
        let mut command = Command::new(&config.codex_executable);
        configure_local_worker_command(&mut command, &config.codex_executable);
        let worker = command
            .arg("--version")
            .current_dir(&config.workspace)
            .output()
            .await
            .map_err(|error| format!("Could not launch Mac-local worker: {error}"))?;
        let success = workspace_ok && worker.status.success();
        let worker_name = match config.engine {
            WorkerEngine::Codex => "Codex",
            WorkerEngine::Claude => "Claude Code",
        };
        let mut text = format!(
            "Mac workspace: {}\nMac {worker_name}: {}\nADB: {}\nXcode simctl: {}",
            if workspace_ok { "ok" } else { "missing" },
            String::from_utf8_lossy(&worker.stdout).trim(),
            if std::path::Path::new("/opt/homebrew/bin/adb").is_file()
                || std::path::Path::new("/usr/local/bin/adb").is_file()
            {
                "available (optional)"
            } else {
                "not installed (optional)"
            },
            if std::path::Path::new("/usr/bin/xcrun").is_file() {
                "available through /usr/bin/xcrun"
            } else {
                "not installed (optional)"
            }
        );

        if !settings.ssh_target.trim().is_empty() {
            let mut remote = config;
            remote.host = WorkerHost::RemoteSsh;
            remote.engine = WorkerEngine::Codex;
            settings.workspace.trim().clone_into(&mut remote.workspace);
            settings
                .codex_executable
                .trim()
                .clone_into(&mut remote.codex_executable);
            remote.sandbox = settings.sandbox;
            match build_probe_script(&remote) {
                Ok(script) => {
                    let output = Command::new("/usr/bin/ssh")
                        .args(ssh_args(&remote.ssh_target, &script))
                        .output()
                        .await
                        .map_err(|error| format!("Could not launch SSH preflight: {error}"))?;
                    text.push_str("\nOptional SSH source link: ");
                    text.push_str(if output.status.success() {
                        "ok"
                    } else {
                        "failed"
                    });
                    let detail = combined_output(&output);
                    if !detail.is_empty() {
                        text.push('\n');
                        text.push_str(&detail);
                    }
                }
                Err(error) => {
                    let _ = write!(text, "\nOptional SSH source link: {error}");
                }
            }
        }
        return Ok(ProbeResult {
            success,
            output: text,
        });
    }
    let script = build_probe_script(&config).map_err(|error| error.to_string())?;
    let output = Command::new("/usr/bin/ssh")
        .args(ssh_args(&config.ssh_target, &script))
        .output()
        .await
        .map_err(|error| format!("Could not launch SSH: {error}"))?;
    let text = combined_output(&output);
    Ok(ProbeResult {
        success: output.status.success(),
        output: text,
    })
}

fn combined_output(output: &std::process::Output) -> String {
    let mut text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let stderr_lossy = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr_lossy.trim();
    if !stderr.is_empty() {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(stderr);
    }
    text
}

fn send(state: &AppState, command: CoordinatorCommand) -> Result<(), String> {
    state
        .commands
        .send(command)
        .map_err(|_| "voice coordinator is unavailable".to_owned())
}
