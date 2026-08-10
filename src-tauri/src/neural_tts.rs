#![cfg(target_os = "macos")]

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct PendingSpeech {
    pub id: u64,
    pub text: String,
    pub rate: f32,
}

#[derive(Debug, Clone)]
pub enum NeuralTtsEvent {
    Loading(String),
    Warming(String),
    Ready(String),
    Generating {
        id: u64,
        tokens: usize,
        elapsed_ms: u64,
    },
    Generated {
        id: u64,
        detail: String,
        tokens: usize,
        elapsed_ms: u64,
    },
    Started(u64),
    Paused(u64),
    Resumed(u64),
    Finished(u64),
    Cancelled(u64),
    Failed {
        id: Option<u64>,
        detail: String,
    },
    Log(String),
    Stopped(String),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HelperEvent {
    event: String,
    id: Option<u64>,
    detail: Option<String>,
    tokens: Option<usize>,
    elapsed_ms: Option<u64>,
}

pub struct NeuralTts {
    child: Child,
    input: ChildStdin,
    events: Receiver<NeuralTtsEvent>,
    ready: bool,
    speaking: bool,
    paused: bool,
    next_id: u64,
    pending: Option<PendingSpeech>,
}

impl NeuralTts {
    pub fn spawn() -> Result<Self, String> {
        let helper_path = helper_path().ok_or_else(|| {
            "The bundled Kokoro helper was not found; Apple speech will be used".to_owned()
        })?;
        let mut child = Command::new(&helper_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| {
                format!(
                    "Could not launch the Kokoro helper at {}: {error}",
                    helper_path.display()
                )
            })?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| "Could not open the Kokoro command pipe".to_owned())?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| "Could not open the Kokoro event pipe".to_owned())?;
        let (event_tx, events) = mpsc::channel();
        thread::Builder::new()
            .name("roadie-kokoro-events".into())
            .spawn(move || {
                let mut last_unstructured = String::new();
                for line in BufReader::new(output).lines() {
                    let event = match line {
                        Ok(line) => parse_event(&line),
                        Err(error) => {
                            NeuralTtsEvent::Stopped(format!("Kokoro event pipe closed: {error}"))
                        }
                    };
                    if let NeuralTtsEvent::Log(detail) = &event {
                        if detail == &last_unstructured {
                            continue;
                        }
                        last_unstructured.clone_from(detail);
                    }
                    if event_tx.send(event).is_err() {
                        return;
                    }
                }
                let _ = event_tx.send(NeuralTtsEvent::Stopped(
                    "Kokoro helper exited; Apple speech remains available".into(),
                ));
            })
            .map_err(|error| format!("Could not start the Kokoro event reader: {error}"))?;

        Ok(Self {
            child,
            input,
            events,
            ready: false,
            speaking: false,
            paused: false,
            next_id: 0,
            pending: None,
        })
    }

    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    pub const fn is_playing(&self) -> bool {
        self.speaking && !self.paused
    }

    pub const fn is_paused(&self) -> bool {
        self.speaking && self.paused
    }

    pub fn speak(&mut self, text: String, rate: f32) -> Result<u64, String> {
        if !self.ready {
            return Err("Kokoro is still loading".into());
        }
        self.next_id = self.next_id.wrapping_add(1);
        let id = self.next_id;
        self.send(&json!({
            "command": "speak",
            "id": id,
            "text": text,
            "rate": rate,
        }))?;
        self.pending = Some(PendingSpeech { id, text, rate });
        Ok(id)
    }

    pub fn pause(&mut self) -> Result<(), String> {
        self.send(&json!({ "command": "pause" }))
    }

    pub fn resume(&mut self) -> Result<(), String> {
        self.send(&json!({ "command": "continue" }))
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.send(&json!({ "command": "stop" }))
    }

    pub fn poll(&mut self) -> Option<NeuralTtsEvent> {
        let event = self.events.try_recv().ok()?;
        match &event {
            NeuralTtsEvent::Ready(_) => self.ready = true,
            NeuralTtsEvent::Started(id) if self.is_active(*id) => {
                self.speaking = true;
                self.paused = false;
            }
            NeuralTtsEvent::Paused(id) if self.is_active(*id) => {
                self.paused = true;
            }
            NeuralTtsEvent::Resumed(id) if self.is_active(*id) => {
                self.speaking = true;
                self.paused = false;
            }
            NeuralTtsEvent::Finished(id) | NeuralTtsEvent::Cancelled(id) if self.is_active(*id) => {
                self.speaking = false;
                self.paused = false;
                self.pending = None;
            }
            NeuralTtsEvent::Failed { id: None, .. } | NeuralTtsEvent::Stopped(_) => {
                self.ready = false;
                self.speaking = false;
                self.paused = false;
            }
            _ => {}
        }
        Some(event)
    }

    pub fn take_pending(&mut self, id: Option<u64>) -> Option<PendingSpeech> {
        let matches = id.is_none_or(|id| self.is_active(id));
        if !matches {
            return None;
        }
        self.speaking = false;
        self.paused = false;
        self.pending.take()
    }

    pub fn shutdown(&mut self) {
        let _ = self.send(&json!({ "command": "shutdown" }));
        for _ in 0..20 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => thread::sleep(Duration::from_millis(25)),
                Err(_) => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn is_active(&self, id: u64) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.id == id)
    }

    fn send(&mut self, payload: &serde_json::Value) -> Result<(), String> {
        serde_json::to_writer(&mut self.input, payload)
            .map_err(|error| format!("Could not encode a Kokoro command: {error}"))?;
        self.input
            .write_all(b"\n")
            .and_then(|()| self.input.flush())
            .map_err(|error| format!("Could not send a command to Kokoro: {error}"))
    }
}

fn helper_path() -> Option<PathBuf> {
    let bundled = std::env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(|parent| parent.join("roadie-tts")));
    if bundled.as_ref().is_some_and(|path| path.is_file()) {
        return bundled;
    }

    let development =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tts-helper/.build/release/roadie-tts");
    development.is_file().then_some(development)
}

fn parse_event(line: &str) -> NeuralTtsEvent {
    let Ok(event) = serde_json::from_str::<HelperEvent>(line) else {
        return NeuralTtsEvent::Log(line.trim().to_owned());
    };
    let detail = event.detail.unwrap_or_default();
    match event.event.as_str() {
        "loading" => NeuralTtsEvent::Loading(detail),
        "warming" => NeuralTtsEvent::Warming(detail),
        "ready" => NeuralTtsEvent::Ready(detail),
        "generating" => event.id.map_or_else(
            || NeuralTtsEvent::Failed {
                id: None,
                detail: "Kokoro generation event had no request ID".into(),
            },
            |id| NeuralTtsEvent::Generating {
                id,
                tokens: event.tokens.unwrap_or(0),
                elapsed_ms: event.elapsed_ms.unwrap_or(0),
            },
        ),
        "generated" => event.id.map_or_else(
            || NeuralTtsEvent::Failed {
                id: None,
                detail: "Kokoro completion event had no request ID".into(),
            },
            |id| NeuralTtsEvent::Generated {
                id,
                detail,
                tokens: event.tokens.unwrap_or(0),
                elapsed_ms: event.elapsed_ms.unwrap_or(0),
            },
        ),
        "started" => id_event(event.id, NeuralTtsEvent::Started),
        "paused" => id_event(event.id, NeuralTtsEvent::Paused),
        "resumed" => id_event(event.id, NeuralTtsEvent::Resumed),
        "finished" => id_event(event.id, NeuralTtsEvent::Finished),
        "cancelled" => id_event(event.id, NeuralTtsEvent::Cancelled),
        "error" => NeuralTtsEvent::Failed {
            id: event.id,
            detail,
        },
        "stopping" => NeuralTtsEvent::Stopped(detail),
        other => NeuralTtsEvent::Failed {
            id: event.id,
            detail: format!("Unknown Kokoro event: {other}"),
        },
    }
}

fn id_event(id: Option<u64>, constructor: impl FnOnce(u64) -> NeuralTtsEvent) -> NeuralTtsEvent {
    id.map_or_else(
        || NeuralTtsEvent::Failed {
            id: None,
            detail: "Kokoro playback event had no request ID".into(),
        },
        constructor,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_generation_progress() {
        let event = parse_event(r#"{"event":"generating","id":7,"tokens":24,"elapsedMs":410}"#);
        assert!(matches!(
            event,
            NeuralTtsEvent::Generating {
                id: 7,
                tokens: 24,
                elapsed_ms: 410
            }
        ));
    }

    #[test]
    fn unstructured_output_becomes_log() {
        assert!(matches!(
            parse_event("not-json"),
            NeuralTtsEvent::Log(detail) if detail == "not-json"
        ));
    }
}
