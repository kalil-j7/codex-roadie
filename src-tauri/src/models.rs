use std::collections::VecDeque;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use roadie_core::{RemoteConfig, SandboxMode, TokenUsage, WorkerEngine, WorkerHost};
use serde::{Deserialize, Serialize};

pub const MAX_CHAT_MESSAGES: usize = 100;
pub const CLAUDE_WORKER_MODEL: &str = "claude-opus-5";
pub const CLAUDE_WORKER_EFFORT: &str = "xhigh";
pub const DEFAULT_REPORTER_MODEL: &str = "gpt-5.3-codex-spark";
pub const FALLBACK_REPORTER_MODEL: &str = "gpt-5.6-luna";
pub const DEFAULT_REMOTE_WORKSPACE: &str = "/home/developer/projects/example-app";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "these are independent persisted dashboard toggles, not one state machine"
)]
pub struct AppSettings {
    pub ssh_target: String,
    pub workspace: String,
    pub codex_executable: String,
    pub codex_model: String,
    pub codex_reasoning_effort: String,
    pub fast_mode: bool,
    pub deep_relay: bool,
    pub local_worker: bool,
    pub local_worker_engine: WorkerEngine,
    pub local_workspace: String,
    pub local_codex_executable: String,
    pub local_claude_executable: String,
    pub reporter_model: String,
    pub sandbox: SandboxMode,
    pub wake_phrase: String,
    pub locale: String,
    pub speech_rate: f32,
    pub silence_ms: u64,
    pub voice_isolation: bool,
    pub auto_listen: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            ssh_target: String::new(),
            workspace: DEFAULT_REMOTE_WORKSPACE.into(),
            codex_executable: "codex".into(),
            codex_model: String::new(),
            codex_reasoning_effort: String::new(),
            fast_mode: false,
            deep_relay: false,
            local_worker: true,
            local_worker_engine: WorkerEngine::Codex,
            local_workspace: default_local_workspace(),
            local_codex_executable: default_local_codex_executable(),
            local_claude_executable: default_local_claude_executable(),
            reporter_model: DEFAULT_REPORTER_MODEL.into(),
            sandbox: SandboxMode::WorkspaceWrite,
            wake_phrase: "hey codex".into(),
            locale: "en-US".into(),
            speech_rate: 0.60,
            silence_ms: 600,
            voice_isolation: true,
            auto_listen: false,
        }
    }
}

impl AppSettings {
    pub fn remote_config(&self) -> RemoteConfig {
        self.remote_config_for_engine(self.local_worker_engine)
    }

    pub fn remote_config_for_engine(&self, requested_engine: WorkerEngine) -> RemoteConfig {
        let selected_engine = if self.local_worker {
            requested_engine
        } else {
            WorkerEngine::Codex
        };
        let (model, reasoning_effort, fast_mode) = match selected_engine {
            WorkerEngine::Codex => (
                self.codex_model.trim(),
                self.codex_reasoning_effort.trim(),
                self.fast_mode,
            ),
            WorkerEngine::Claude => (CLAUDE_WORKER_MODEL, CLAUDE_WORKER_EFFORT, false),
        };
        let (host, engine, workspace, codex_executable, sandbox) = if self.local_worker {
            (
                WorkerHost::LocalMac,
                selected_engine,
                self.local_workspace.trim(),
                match selected_engine {
                    WorkerEngine::Codex => self.local_codex_executable.trim(),
                    WorkerEngine::Claude => self.local_claude_executable.trim(),
                },
                self.sandbox,
            )
        } else {
            (
                WorkerHost::RemoteSsh,
                WorkerEngine::Codex,
                self.workspace.trim(),
                self.codex_executable.trim(),
                self.sandbox,
            )
        };
        RemoteConfig {
            host,
            engine,
            ssh_target: self.ssh_target.trim().to_owned(),
            workspace: workspace.to_owned(),
            codex_executable: codex_executable.to_owned(),
            sandbox,
            model: model.to_owned(),
            reasoning_effort: reasoning_effort.to_owned(),
            fast_mode,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.workspace.trim().is_empty() {
            validate_workspace("Remote workspace", &self.workspace)?;
        } else if !self.local_worker {
            return Err("Remote workspace is required".into());
        }
        validate_workspace("Mac workspace", &self.local_workspace)?;
        self.remote_config()
            .validate()
            .map_err(|error| error.to_string())?;
        if self.wake_phrase.trim().is_empty() {
            return Err("Wake phrase is required".into());
        }
        if self.locale.trim().is_empty()
            || !self
                .locale
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '-')
        {
            return Err("Locale must be a BCP-47 value such as en-US".into());
        }
        if !(0.1..=1.0).contains(&self.speech_rate) {
            return Err("Speech rate must be between 0.1 and 1.0".into());
        }
        if !(500..=3_000).contains(&self.silence_ms) {
            return Err("Silence timeout must be between 500 and 3000 milliseconds".into());
        }
        if self.local_codex_executable.trim().is_empty()
            || !self.local_codex_executable.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '/' | '.' | '-' | '_')
            })
        {
            return Err(
                "Local reporter Codex must be a command name or absolute path without spaces"
                    .into(),
            );
        }
        if self.reporter_model.trim().is_empty()
            || self.reporter_model.len() > 120
            || !self.reporter_model.chars().all(|character| {
                character.is_ascii_alphanumeric()
                    || matches!(character, '.' | '-' | '_' | '/' | ':')
            })
        {
            return Err("Reporter model contains unsupported characters".into());
        }
        Ok(())
    }

    pub fn reporter_fallback_model(&self) -> &'static str {
        if self.reporter_model.trim() == FALLBACK_REPORTER_MODEL {
            DEFAULT_REPORTER_MODEL
        } else {
            FALLBACK_REPORTER_MODEL
        }
    }
}

fn validate_workspace(label: &str, workspace: &str) -> Result<(), String> {
    let workspace = workspace.trim();
    if workspace.is_empty() {
        return Err(format!("{label} is required"));
    }
    if !workspace.starts_with('/') {
        return Err(format!("{label} must be an absolute path"));
    }
    if workspace.len() > 4_096 || workspace.chars().any(char::is_control) {
        return Err(format!("{label} contains unsupported characters"));
    }
    Ok(())
}

fn default_local_codex_executable() -> String {
    let mut candidates = vec![
        "/opt/homebrew/bin/codex".to_owned(),
        "/usr/local/bin/codex".to_owned(),
    ];
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            std::path::Path::new(&home)
                .join(".local/bin/codex")
                .to_string_lossy()
                .into_owned(),
        );
    }
    candidates
        .into_iter()
        .find(|candidate| std::path::Path::new(candidate).is_file())
        .unwrap_or_else(|| "codex".into())
}

fn default_local_claude_executable() -> String {
    let mut candidates = Vec::new();
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            std::path::Path::new(&home)
                .join(".local/bin/claude")
                .to_string_lossy()
                .into_owned(),
        );
    }
    candidates.extend([
        "/opt/homebrew/bin/claude".to_owned(),
        "/usr/local/bin/claude".to_owned(),
    ]);
    candidates
        .into_iter()
        .find(|candidate| std::path::Path::new(candidate).is_file())
        .unwrap_or_else(|| "claude".into())
}

fn default_local_workspace() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/Users/Shared".into())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PipelineStage {
    #[default]
    Loading,
    Listening,
    Capturing,
    Transcribing,
    Transferring,
    Processing,
    Reporting,
    Responding,
    Reconnecting,
    Paused,
    Error,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectivityState {
    #[default]
    Online,
    Reconnecting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    pub id: u64,
    pub role: ChatRole,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ChatRole {
    User,
    Agent,
    Worker,
    Reporter,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewState {
    pub status: String,
    pub listening: bool,
    pub speaking: bool,
    pub working: bool,
    pub active_workers: usize,
    pub connectivity: ConnectivityState,
    pub reconnect_attempt: u32,
    pub queued_additions: usize,
    pub session_id: Option<String>,
    pub live_transcript: String,
    pub last_error: Option<String>,
    pub pipeline_stage: PipelineStage,
    pub pipeline_detail: String,
    pub audio_level: f32,
    pub last_usage: Option<TokenUsage>,
    pub session_usage: TokenUsage,
    pub last_reporter_usage: Option<TokenUsage>,
    pub session_reporter_usage: TokenUsage,
    pub messages: VecDeque<ChatMessage>,
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            status: "Starting".into(),
            listening: false,
            speaking: false,
            working: false,
            active_workers: 0,
            connectivity: ConnectivityState::Online,
            reconnect_attempt: 0,
            queued_additions: 0,
            session_id: None,
            live_transcript: String::new(),
            last_error: None,
            pipeline_stage: PipelineStage::Loading,
            pipeline_detail: "Loading local Whisper model".into(),
            audio_level: 0.0,
            last_usage: None,
            session_usage: TokenUsage::default(),
            last_reporter_usage: None,
            session_reporter_usage: TokenUsage::default(),
            messages: VecDeque::new(),
        }
    }
}

impl ViewState {
    pub fn push_message(&mut self, message: ChatMessage) {
        self.messages.push_back(message);
        while self.messages.len() > MAX_CHAT_MESSAGES {
            self.messages.pop_front();
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Bootstrap {
    pub settings: AppSettings,
    pub view: ViewState,
}

#[derive(Debug)]
pub struct SharedModel {
    pub settings: AppSettings,
    pub view: ViewState,
}

pub type Shared = Arc<RwLock<SharedModel>>;

pub fn read_shared(shared: &Shared) -> RwLockReadGuard<'_, SharedModel> {
    shared
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub fn write_shared(shared: &Shared) -> RwLockWriteGuard<'_, SharedModel> {
    shared
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mac_worker_uses_the_selected_profile_and_safe_default_access() {
        let settings = AppSettings::default();
        let config = settings.remote_config();
        assert_eq!(config.host, WorkerHost::LocalMac);
        assert_eq!(config.engine, WorkerEngine::Codex);
        assert!(config.model.is_empty());
        assert!(config.reasoning_effort.is_empty());
        assert!(!config.fast_mode);
        assert_eq!(config.sandbox, SandboxMode::WorkspaceWrite);
    }

    #[test]
    fn mac_worker_can_use_claude_opus_xhigh_with_device_access() {
        let settings = AppSettings {
            local_worker_engine: WorkerEngine::Claude,
            local_claude_executable: "/Users/example/.local/bin/claude".into(),
            sandbox: SandboxMode::DangerFullAccess,
            ..AppSettings::default()
        };
        let config = settings.remote_config();
        assert_eq!(config.host, WorkerHost::LocalMac);
        assert_eq!(config.engine, WorkerEngine::Claude);
        assert_eq!(config.codex_executable, "/Users/example/.local/bin/claude");
        assert_eq!(config.model, CLAUDE_WORKER_MODEL);
        assert_eq!(config.reasoning_effort, CLAUDE_WORKER_EFFORT);
        assert!(!config.fast_mode);
        assert_eq!(config.sandbox, SandboxMode::DangerFullAccess);
    }

    #[test]
    fn voice_route_can_select_either_mac_worker_without_changing_the_default() {
        let settings = AppSettings {
            local_worker_engine: WorkerEngine::Claude,
            local_codex_executable: "/opt/homebrew/bin/codex".into(),
            local_claude_executable: "/Users/example/.local/bin/claude".into(),
            ..AppSettings::default()
        };

        let codex = settings.remote_config_for_engine(WorkerEngine::Codex);
        let claude = settings.remote_config_for_engine(WorkerEngine::Claude);

        assert_eq!(settings.local_worker_engine, WorkerEngine::Claude);
        assert_eq!(codex.engine, WorkerEngine::Codex);
        assert_eq!(codex.codex_executable, "/opt/homebrew/bin/codex");
        assert!(codex.model.is_empty());
        assert_eq!(claude.engine, WorkerEngine::Claude);
        assert_eq!(claude.codex_executable, "/Users/example/.local/bin/claude");
        assert_eq!(claude.model, CLAUDE_WORKER_MODEL);
    }

    #[test]
    fn remote_worker_keeps_the_configured_repository_sandbox() {
        let settings = AppSettings {
            local_worker: false,
            ssh_target: "build-host".into(),
            codex_model: "gpt-5.4".into(),
            codex_reasoning_effort: "high".into(),
            fast_mode: false,
            ..AppSettings::default()
        };
        let config = settings.remote_config();
        assert_eq!(config.host, WorkerHost::RemoteSsh);
        assert_eq!(config.engine, WorkerEngine::Codex);
        assert_eq!(config.model, "gpt-5.4");
        assert_eq!(config.reasoning_effort, "high");
        assert_eq!(config.sandbox, SandboxMode::WorkspaceWrite);
    }

    #[test]
    fn default_remote_workspace_is_a_neutral_setup_placeholder() {
        assert_eq!(AppSettings::default().workspace, DEFAULT_REMOTE_WORKSPACE);
    }

    #[test]
    fn workspace_presets_are_absolute_and_host_specific() {
        let mut settings = AppSettings {
            local_worker: false,
            ssh_target: "build-host".into(),
            workspace: "/home/developer/projects/another-project".into(),
            ..AppSettings::default()
        };
        settings.validate().unwrap();
        assert_eq!(
            settings.remote_config().workspace,
            "/home/developer/projects/another-project"
        );

        settings.local_worker = true;
        settings.local_workspace = "/Users/example/projects/another-project".into();
        settings.validate().unwrap();
        assert_eq!(
            settings.remote_config().workspace,
            "/Users/example/projects/another-project"
        );
        assert_eq!(
            settings.workspace,
            "/home/developer/projects/another-project"
        );
    }

    #[test]
    fn workspace_presets_reject_relative_or_control_character_paths() {
        let mut settings = AppSettings {
            local_worker: false,
            ssh_target: "build-host".into(),
            workspace: "projects/example-app".into(),
            ..AppSettings::default()
        };
        assert_eq!(
            settings.validate().unwrap_err(),
            "Remote workspace must be an absolute path"
        );

        settings.workspace = "/home/developer/projects/example-app\nother".into();
        assert_eq!(
            settings.validate().unwrap_err(),
            "Remote workspace contains unsupported characters"
        );
    }

    #[test]
    fn existing_settings_migrate_to_the_mac_worker() {
        let settings: AppSettings = serde_json::from_str(
            r#"{
                "sshTarget":"build-host",
                "workspace":"/home/developer/projects/example-app",
                "codexExecutable":"codex",
                "localCodexExecutable":"/opt/homebrew/bin/codex",
                "deepRelay":true
            }"#,
        )
        .unwrap();
        assert!(settings.local_worker);
        assert_eq!(settings.local_worker_engine, WorkerEngine::Codex);
        assert!(settings.voice_isolation);
        assert!(!settings.local_claude_executable.is_empty());
        assert!(!settings.local_workspace.is_empty());
        assert_eq!(settings.remote_config().host, WorkerHost::LocalMac);
    }

    #[test]
    fn saved_settings_can_select_the_remote_worker() {
        let settings = AppSettings {
            local_worker: false,
            ssh_target: "build-host".into(),
            codex_model: "gpt-5.4-mini".into(),
            codex_reasoning_effort: "minimal".into(),
            fast_mode: false,
            ..AppSettings::default()
        };
        let config = settings.remote_config();
        assert!(!settings.local_worker);
        assert_eq!(config.host, WorkerHost::RemoteSsh);
        assert_eq!(config.model, "gpt-5.4-mini");
        assert_eq!(config.reasoning_effort, "minimal");
        assert!(!config.fast_mode);
        assert_eq!(config.sandbox, SandboxMode::WorkspaceWrite);
    }
}
