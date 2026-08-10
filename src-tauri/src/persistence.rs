use std::fs;
use std::path::Path;

use roadie_core::{TokenUsage, WorkerEngine, WorkerHost};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::models::AppSettings;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingRequest {
    pub prompt: String,
    #[serde(default)]
    pub additions: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeState {
    pub session_id: Option<String>,
    #[serde(default)]
    pub session_host: Option<WorkerHost>,
    #[serde(default)]
    pub session_engine: Option<WorkerEngine>,
    #[serde(default)]
    pub session_usage: TokenUsage,
    #[serde(default)]
    pub reporter_usage: TokenUsage,
    #[serde(default)]
    pub pending_request: Option<PendingRequest>,
}

pub fn load_settings(path: &Path) -> AppSettings {
    load_json(path).unwrap_or_default()
}

pub fn load_runtime(path: &Path) -> RuntimeState {
    load_json(path).unwrap_or_default()
}

pub fn save_settings(path: &Path, settings: &AppSettings) -> Result<(), String> {
    save_json(path, settings)
}

pub fn save_runtime(path: &Path, runtime: &RuntimeState) -> Result<(), String> {
    save_json(path, runtime)
}

fn load_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let contents = fs::read_to_string(path).ok()?;
    serde_json::from_str(&contents).ok()
}

fn save_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "configuration path has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "codex-roadie-{name}-{}-runtime.json",
            std::process::id()
        ))
    }

    #[test]
    fn pending_request_survives_runtime_round_trip() {
        let path = test_path("pending");
        let runtime = RuntimeState {
            session_host: Some(WorkerHost::LocalMac),
            session_engine: Some(WorkerEngine::Claude),
            pending_request: Some(PendingRequest {
                prompt: "Finish the QA pass".into(),
                additions: vec!["Also check reconnect".into()],
            }),
            ..RuntimeState::default()
        };
        save_runtime(&path, &runtime).unwrap();
        let loaded = load_runtime(&path);
        assert_eq!(loaded.session_host, Some(WorkerHost::LocalMac));
        assert_eq!(loaded.session_engine, Some(WorkerEngine::Claude));
        let pending = loaded.pending_request.unwrap();
        assert_eq!(pending.prompt, "Finish the QA pass");
        assert_eq!(pending.additions, ["Also check reconnect"]);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn older_runtime_without_pending_request_migrates_cleanly() {
        let path = test_path("legacy");
        fs::write(&path, r#"{"sessionId":"thread-123"}"#).unwrap();
        let loaded = load_runtime(&path);
        assert_eq!(loaded.session_id.as_deref(), Some("thread-123"));
        assert!(loaded.pending_request.is_none());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn remote_worker_choice_survives_settings_round_trip() {
        let path = test_path("remote-worker-settings");
        let settings = AppSettings {
            local_worker: false,
            ssh_target: "build-host".into(),
            ..AppSettings::default()
        };
        save_settings(&path, &settings).unwrap();
        let loaded = load_settings(&path);
        assert!(!loaded.local_worker);
        assert_eq!(loaded.ssh_target, "build-host");
        assert_eq!(loaded.remote_config().host, WorkerHost::RemoteSsh);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn selected_remote_workspace_survives_settings_round_trip() {
        let path = test_path("remote-workspace-preset");
        let settings = AppSettings {
            local_worker: false,
            ssh_target: "build-host".into(),
            workspace: "/home/developer/projects/example-app".into(),
            ..AppSettings::default()
        };
        save_settings(&path, &settings).unwrap();
        let loaded = load_settings(&path);
        assert_eq!(loaded.workspace, "/home/developer/projects/example-app");
        assert_eq!(loaded.remote_config().workspace, loaded.workspace);
        let _ = fs::remove_file(path);
    }
}
