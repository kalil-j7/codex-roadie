use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    ReadOnly,
    #[default]
    WorkspaceWrite,
    DangerFullAccess,
}

impl SandboxMode {
    pub const fn as_cli_value(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }

    pub const fn as_app_server_value(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerHost {
    LocalMac,
    #[default]
    RemoteSsh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerEngine {
    #[default]
    Codex,
    Claude,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteConfig {
    pub host: WorkerHost,
    pub engine: WorkerEngine,
    pub ssh_target: String,
    pub workspace: String,
    pub codex_executable: String,
    pub sandbox: SandboxMode,
    pub model: String,
    pub reasoning_effort: String,
    pub fast_mode: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RemoteConfigError {
    #[error("SSH target is required")]
    MissingTarget,
    #[error("SSH target contains unsupported characters")]
    InvalidTarget,
    #[error("worker workspace is required")]
    MissingWorkspace,
    #[error("worker executable is required")]
    MissingExecutable,
    #[error("worker executable must be a command name or absolute path without spaces")]
    InvalidExecutable,
    #[error("worker model contains unsupported characters")]
    InvalidModel,
    #[error("worker reasoning effort must be default, minimal, low, medium, high, or xhigh")]
    InvalidReasoningEffort,
    #[error("session id contains unsupported characters")]
    InvalidSessionId,
    #[error("Claude Code is currently supported only by the Mac-local worker")]
    UnsupportedEngineHost,
    #[error("Claude Code requires explicitly selecting Full Mac access in this release")]
    ClaudeRequiresFullAccess,
}

impl RemoteConfig {
    pub fn validate(&self) -> Result<(), RemoteConfigError> {
        if self.host == WorkerHost::RemoteSsh {
            if self.engine == WorkerEngine::Claude {
                return Err(RemoteConfigError::UnsupportedEngineHost);
            }
            if self.ssh_target.trim().is_empty() {
                return Err(RemoteConfigError::MissingTarget);
            }
            if self.ssh_target.starts_with('-')
                || !self.ssh_target.chars().all(|character| {
                    character.is_ascii_alphanumeric()
                        || matches!(character, '.' | '-' | '_' | '@' | ':' | '[' | ']')
                })
            {
                return Err(RemoteConfigError::InvalidTarget);
            }
        }
        if self.engine == WorkerEngine::Claude && self.sandbox != SandboxMode::DangerFullAccess {
            return Err(RemoteConfigError::ClaudeRequiresFullAccess);
        }
        if self.workspace.trim().is_empty() {
            return Err(RemoteConfigError::MissingWorkspace);
        }
        if self.codex_executable.trim().is_empty() {
            return Err(RemoteConfigError::MissingExecutable);
        }
        if !self.codex_executable.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '/' | '.' | '-' | '_')
        }) {
            return Err(RemoteConfigError::InvalidExecutable);
        }
        if self.model.len() > 120
            || !self.model.chars().all(|character| {
                character.is_ascii_alphanumeric()
                    || matches!(character, '.' | '-' | '_' | '/' | ':')
            })
        {
            return Err(RemoteConfigError::InvalidModel);
        }
        if !matches!(
            self.reasoning_effort.as_str(),
            "" | "minimal" | "low" | "medium" | "high" | "xhigh"
        ) {
            return Err(RemoteConfigError::InvalidReasoningEffort);
        }
        Ok(())
    }
}

pub fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_owned();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn build_codex_script(
    config: &RemoteConfig,
    session_id: Option<&str>,
) -> Result<String, RemoteConfigError> {
    config.validate()?;
    if let Some(session_id) = session_id {
        validate_session_id(session_id)?;
    }

    let cd = format!("cd -- {}", shell_quote(&config.workspace));
    let codex = shell_quote(&config.codex_executable);
    let mut profile = String::new();
    if !config.model.is_empty() {
        profile.push_str(" --model ");
        profile.push_str(&shell_quote(&config.model));
    }
    if !config.reasoning_effort.is_empty() {
        profile.push_str(" -c ");
        profile.push_str(&shell_quote(&format!(
            "model_reasoning_effort=\"{}\"",
            config.reasoning_effort
        )));
    }
    if config.fast_mode {
        profile.push_str(" -c ");
        profile.push_str(&shell_quote("service_tier=\"fast\""));
    }
    let command = match session_id {
        Some(session_id) => format!(
            "{codex} exec resume --json{profile} -c {} -c {} {} -",
            shell_quote("approval_policy=\"never\""),
            shell_quote(&format!(
                "sandbox_mode=\"{}\"",
                config.sandbox.as_cli_value()
            )),
            shell_quote(session_id)
        ),
        None => format!(
            "{codex} exec --json --color never --sandbox {}{profile} -c {} -",
            config.sandbox.as_cli_value(),
            shell_quote("approval_policy=\"never\"")
        ),
    };

    Ok(format!("{cd} && exec {command}"))
}

pub fn build_app_server_script(config: &RemoteConfig) -> Result<String, RemoteConfigError> {
    config.validate()?;
    let cd = format!("cd -- {}", shell_quote(&config.workspace));
    let codex = shell_quote(&config.codex_executable);
    Ok(format!("{cd} && exec {codex} app-server --stdio"))
}

pub fn build_probe_script(config: &RemoteConfig) -> Result<String, RemoteConfigError> {
    config.validate()?;
    let workspace = shell_quote(&config.workspace);
    let codex = shell_quote(&config.codex_executable);
    Ok(format!(
        "test -d {workspace} && printf 'workspace: ok\\n' && {codex} --version"
    ))
}

pub fn ssh_args(target: &str, script: &str) -> Vec<String> {
    vec![
        "-T".into(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "ServerAliveInterval=15".into(),
        "-o".into(),
        "ServerAliveCountMax=2".into(),
        "-o".into(),
        "ControlMaster=auto".into(),
        "-o".into(),
        "ControlPersist=600".into(),
        "-o".into(),
        "ControlPath=/tmp/codex-roadie-%C".into(),
        "--".into(),
        target.into(),
        format!("bash -lc {}", shell_quote(script)),
    ]
}

fn validate_session_id(session_id: &str) -> Result<(), RemoteConfigError> {
    if session_id.is_empty()
        || !session_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(RemoteConfigError::InvalidSessionId);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RemoteConfig {
        RemoteConfig {
            host: WorkerHost::RemoteSsh,
            engine: WorkerEngine::Codex,
            ssh_target: "build-host".into(),
            workspace: "/home/developer/projects/example-app".into(),
            codex_executable: "/home/developer/.local/bin/codex".into(),
            sandbox: SandboxMode::WorkspaceWrite,
            model: "gpt-5.6-sol".into(),
            reasoning_effort: "high".into(),
            fast_mode: true,
        }
    }

    #[test]
    fn quotes_untrusted_shell_values() {
        assert_eq!(shell_quote("it's here"), "'it'\\''s here'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn new_session_uses_json_workspace_write_and_stdin() {
        let script = build_codex_script(&config(), None).unwrap();
        assert!(script.contains("&& exec "));
        assert!(script.contains("exec --json --color never --sandbox workspace-write"));
        assert!(script.ends_with("-"));
        assert!(script.contains("approval_policy=\"never\""));
        assert!(script.contains("--model 'gpt-5.6-sol'"));
        assert!(script.contains("service_tier=\"fast\""));
        assert!(script.contains("model_reasoning_effort=\"high\""));
    }

    #[test]
    fn resumed_session_uses_exact_id() {
        let script =
            build_codex_script(&config(), Some("0199a213-81c0-7800-8aa1-bbab2a035a53")).unwrap();
        assert!(script.contains("&& exec "));
        assert!(script.contains("exec resume --json"));
        assert!(script.contains("sandbox_mode=\"workspace-write\""));
        assert!(script.contains("'0199a213-81c0-7800-8aa1-bbab2a035a53' -"));
    }

    #[test]
    fn app_server_uses_the_configured_workspace_and_executable() {
        let script = build_app_server_script(&config()).unwrap();
        assert_eq!(
            script,
            "cd -- '/home/developer/projects/example-app' && exec '/home/developer/.local/bin/codex' app-server --stdio"
        );
        assert_eq!(
            SandboxMode::WorkspaceWrite.as_app_server_value(),
            "workspace-write"
        );
    }

    #[test]
    fn target_cannot_become_an_ssh_option() {
        let mut bad = config();
        bad.ssh_target = "-oProxyCommand=oops".into();
        assert_eq!(bad.validate(), Err(RemoteConfigError::InvalidTarget));
    }

    #[test]
    fn local_worker_needs_no_ssh_target_and_supports_device_access() {
        let mut local = config();
        local.host = WorkerHost::LocalMac;
        local.ssh_target.clear();
        local.workspace = "/Users/example".into();
        local.codex_executable = "/opt/homebrew/bin/codex".into();
        local.sandbox = SandboxMode::DangerFullAccess;
        assert_eq!(local.validate(), Ok(()));
        assert_eq!(local.sandbox.as_app_server_value(), "danger-full-access");
    }

    #[test]
    fn claude_requires_an_explicit_full_access_choice() {
        let mut local = config();
        local.host = WorkerHost::LocalMac;
        local.engine = WorkerEngine::Claude;
        local.ssh_target.clear();
        local.workspace = "/Users/example".into();
        local.codex_executable = "claude".into();
        assert_eq!(
            local.validate(),
            Err(RemoteConfigError::ClaudeRequiresFullAccess)
        );

        local.sandbox = SandboxMode::DangerFullAccess;
        assert_eq!(local.validate(), Ok(()));
    }
}
