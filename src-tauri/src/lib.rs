mod commands;
mod coordinator;
mod models;
#[cfg(target_os = "macos")]
mod neural_tts;
mod persistence;
mod process_env;
mod remote_runner;
mod speech_runtime;

use std::sync::{Arc, Mutex, RwLock};

use commands::AppState;
use coordinator::Coordinator;
use models::{SharedModel, ViewState};
use persistence::{load_runtime, load_settings};
use tauri::Manager;
use tokio::sync::mpsc;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
/// Starts the Codex Roadie desktop application.
///
/// # Panics
///
/// Panics if Tauri cannot initialize or run the native application event loop.
pub fn run() {
    let speech_shutdown = Arc::new(Mutex::new(None));
    let setup_speech_shutdown = speech_shutdown.clone();
    let app = tauri::Builder::default()
        .setup(move |app| {
            let config_dir = app.path().app_config_dir()?;
            let settings_path = config_dir.join("settings.json");
            let runtime_path = config_dir.join("runtime.json");
            let settings = load_settings(&settings_path);
            let runtime = load_runtime(&runtime_path);
            let worker_config = settings.remote_config();
            let runtime_engine = runtime.session_engine.unwrap_or(worker_config.engine);
            let runtime_worker_config = settings.remote_config_for_engine(runtime_engine);
            let runtime_route_is_supported = runtime.session_host
                == Some(runtime_worker_config.host)
                && (settings.local_worker || runtime_engine == roadie_core::WorkerEngine::Codex);
            let session_engine = runtime_route_is_supported.then_some(runtime_engine);
            let session_id = runtime_route_is_supported
                .then(|| runtime.session_id.clone())
                .flatten();
            let pending_request = runtime_route_is_supported
                .then(|| runtime.pending_request.clone())
                .flatten();

            let mut view = ViewState::default();
            view.session_id.clone_from(&session_id);
            view.session_usage = runtime.session_usage;
            view.session_reporter_usage = runtime.reporter_usage;
            let shared = Arc::new(RwLock::new(SharedModel {
                settings: settings.clone(),
                view,
            }));

            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let model_path = config_dir
                .join("models")
                .join("ggml-large-v3-turbo-q5_0.bin");
            let (speech, speech_rx) = speech_runtime::spawn(model_path);
            *setup_speech_shutdown
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(speech.clone());
            let (agent_tx, agent_rx) = mpsc::unbounded_channel();
            let coordinator = Coordinator::new(
                app.handle().clone(),
                shared.clone(),
                settings_path.clone(),
                runtime_path,
                settings,
                session_id,
                session_engine,
                pending_request,
                command_rx,
                speech.clone(),
                speech_rx,
                agent_rx,
                agent_tx,
            );
            tauri::async_runtime::spawn(coordinator.run());

            app.manage(AppState {
                shared,
                commands: command_tx,
                settings_path,
            });

            keep_mac_awake();
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::bootstrap,
            commands::save_app_settings,
            commands::send_text_prompt,
            commands::start_listening,
            commands::stop_listening,
            commands::new_session,
            commands::cancel_turn,
            commands::stop_speaking,
            commands::repeat_last,
            commands::test_connection,
        ])
        .build(tauri::generate_context!())
        .expect("error while building Codex Roadie");
    let callback_speech_shutdown = speech_shutdown.clone();
    let exit_code = app.run_return(move |_, event| {
        if matches!(
            event,
            tauri::RunEvent::Exit | tauri::RunEvent::ExitRequested { .. }
        ) {
            shutdown_speech(&callback_speech_shutdown);
        }
    });
    shutdown_speech(&speech_shutdown);
    std::process::exit(exit_code);
}

fn shutdown_speech(shutdown: &Arc<Mutex<Option<speech_runtime::SpeechHandle>>>) {
    let speech = shutdown
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .cloned();
    if let Some(speech) = speech {
        speech.shutdown_and_join();
    }
}

#[cfg(target_os = "macos")]
fn keep_mac_awake() {
    let pid = std::process::id().to_string();
    let _ = std::process::Command::new("/usr/bin/caffeinate")
        .args(["-i", "-w", pid.as_str()])
        .spawn();
}

#[cfg(not(target_os = "macos"))]
fn keep_mac_awake() {}
