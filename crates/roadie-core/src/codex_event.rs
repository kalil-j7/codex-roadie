use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
}

impl TokenUsage {
    pub const fn total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }

    pub fn accumulate(&mut self, usage: Self) {
        self.input_tokens = self.input_tokens.saturating_add(usage.input_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(usage.cached_input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(usage.output_tokens);
        self.reasoning_output_tokens = self
            .reasoning_output_tokens
            .saturating_add(usage.reasoning_output_tokens);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexEvent {
    ThreadStarted(String),
    TurnStarted,
    AgentMessage(String),
    TurnCompleted(TokenUsage),
    TurnFailed(String),
    Error(String),
    Ignored,
}

pub fn parse_codex_event(line: &str) -> Result<CodexEvent, serde_json::Error> {
    let value: Value = serde_json::from_str(line)?;
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();

    let event = match event_type {
        "thread.started" => value
            .get("thread_id")
            .and_then(Value::as_str)
            .map(|id| CodexEvent::ThreadStarted(id.to_owned()))
            .unwrap_or(CodexEvent::Ignored),
        "turn.started" => CodexEvent::TurnStarted,
        "item.completed" => {
            let item = value.get("item").unwrap_or(&Value::Null);
            if item.get("type").and_then(Value::as_str) == Some("agent_message") {
                item.get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                    .map(|text| CodexEvent::AgentMessage(text.to_owned()))
                    .unwrap_or(CodexEvent::Ignored)
            } else {
                CodexEvent::Ignored
            }
        }
        "turn.completed" => CodexEvent::TurnCompleted(parse_usage(&value)),
        "turn.failed" => CodexEvent::TurnFailed(extract_message(&value, "Codex turn failed")),
        "error" => CodexEvent::Error(extract_message(&value, "Codex reported an error")),
        _ => CodexEvent::Ignored,
    };

    Ok(event)
}

fn parse_usage(value: &Value) -> TokenUsage {
    let usage = value.get("usage").unwrap_or(&Value::Null);
    TokenUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        cached_input_tokens: usage
            .get("cached_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        reasoning_output_tokens: usage
            .get("reasoning_output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    }
}

fn extract_message(value: &Value, fallback: &str) -> String {
    value
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
        })
        .unwrap_or(fallback)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_agent_chat_messages_only() {
        let event = parse_codex_event(
            r#"{"type":"item.completed","item":{"id":"i","type":"agent_message","text":"Done."}}"#,
        )
        .unwrap();
        assert_eq!(event, CodexEvent::AgentMessage("Done.".into()));

        let command = parse_codex_event(
            r#"{"type":"item.completed","item":{"type":"command_execution","command":"ls"}}"#,
        )
        .unwrap();
        assert_eq!(command, CodexEvent::Ignored);
    }

    #[test]
    fn parses_thread_and_failure_events() {
        assert_eq!(
            parse_codex_event(r#"{"type":"thread.started","thread_id":"abc-123"}"#).unwrap(),
            CodexEvent::ThreadStarted("abc-123".into())
        );
        assert_eq!(
            parse_codex_event(r#"{"type":"turn.failed","error":{"message":"nope"}}"#).unwrap(),
            CodexEvent::TurnFailed("nope".into())
        );
    }

    #[test]
    fn parses_turn_usage() {
        let event = parse_codex_event(
            r#"{"type":"turn.completed","usage":{"input_tokens":24763,"cached_input_tokens":24448,"output_tokens":122,"reasoning_output_tokens":17}}"#,
        )
        .unwrap();
        assert_eq!(
            event,
            CodexEvent::TurnCompleted(TokenUsage {
                input_tokens: 24_763,
                cached_input_tokens: 24_448,
                output_tokens: 122,
                reasoning_output_tokens: 17,
            })
        );
    }
}
