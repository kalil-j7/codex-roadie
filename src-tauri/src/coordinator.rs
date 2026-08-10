use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use roadie_core::{SandboxMode, VoiceIntent, WorkerEngine, WorkerHost, parse_routed_voice_intent};
use tauri::{AppHandle, Emitter};
use tokio::sync::mpsc;

use crate::models::{
    AppSettings, ChatMessage, ChatRole, ConnectivityState, PipelineStage, Shared, ViewState,
    write_shared,
};
use crate::persistence::{PendingRequest, RuntimeState, save_runtime, save_settings};
use crate::remote_runner::{
    AgentControl, AgentEvent, AgentLane, ReporterConfig, spawn_turn, warm_connection,
};
use crate::speech_runtime::{DetectedSpeechSource, SpeechCommand, SpeechEngineEvent, SpeechHandle};

const ARMED_WINDOW: Duration = Duration::from_secs(8);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_WORKERS: usize = 3;

#[derive(Debug)]
pub enum CoordinatorCommand {
    StartListening,
    StopListening,
    SubmitPrompt(String),
    SettingsUpdated(Box<AppSettings>),
    NewSession,
    CancelTurn,
    StopSpeaking,
    RepeatLast,
}

#[derive(Debug, Clone, Copy)]
enum TimedEvent {
    Finalize { generation: u64, revision: u64 },
    Reconnect { turn_id: u64, attempt: u32 },
    ParallelReconnect { turn_id: u64, attempt: u32 },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum InterruptionState {
    #[default]
    None,
    Capturing,
    Paused,
}

impl InterruptionState {
    const fn is_active(self) -> bool {
        !matches!(self, Self::None)
    }
}

const fn should_pause_for_detected_speech(
    during_playback: bool,
    speaking: bool,
    interruption_active: bool,
) -> bool {
    (during_playback || speaking) && !interruption_active
}

fn direct_barge_in_intent(
    transcript: &str,
    intent: VoiceIntent,
    interruption_active: bool,
) -> VoiceIntent {
    if interruption_active && matches!(intent, VoiceIntent::NotAddressed) {
        VoiceIntent::Prompt(transcript.trim().to_owned())
    } else {
        intent
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum CancellationState {
    #[default]
    Idle,
    Requested,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum ParallelVoiceState {
    #[default]
    Enabled,
    Suppressed,
}

#[derive(Debug, Clone)]
struct PendingPrompt {
    text: String,
    engine: WorkerEngine,
}

#[derive(Debug, Clone)]
struct PendingWorkerClarification {
    prompt: String,
    engine: WorkerEngine,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerTarget {
    Primary,
    Parallel(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerReference {
    None,
    Label(u64),
    Ambiguous,
}

#[derive(Debug)]
struct ParallelTurn {
    label: u64,
    engine: WorkerEngine,
    request: PendingRequest,
    session_id: Option<String>,
    control: Option<AgentControl>,
    reconnect_attempt: u32,
    failure: Option<String>,
    had_message: bool,
    committed: bool,
    unconfirmed_additions: VecDeque<String>,
    queued_follow_ups: Vec<String>,
}

pub struct Coordinator {
    app: AppHandle,
    shared: Shared,
    settings_path: PathBuf,
    runtime_path: PathBuf,
    settings: AppSettings,
    session_id: Option<String>,
    session_engine: Option<WorkerEngine>,
    command_rx: mpsc::UnboundedReceiver<CoordinatorCommand>,
    speech: SpeechHandle,
    speech_rx: mpsc::UnboundedReceiver<SpeechEngineEvent>,
    agent_rx: mpsc::UnboundedReceiver<AgentEvent>,
    agent_tx: mpsc::UnboundedSender<AgentEvent>,
    timed_rx: mpsc::UnboundedReceiver<TimedEvent>,
    timed_tx: mpsc::UnboundedSender<TimedEvent>,
    active_generation: u64,
    transcript_revision: u64,
    latest_transcript: String,
    armed_until: Option<Instant>,
    armed_engine: Option<WorkerEngine>,
    speaking: bool,
    interruption: InterruptionState,
    cancellation: CancellationState,
    voice_suppressed_turn: Option<u64>,
    parallel_voice: ParallelVoiceState,
    active_turn: Option<u64>,
    active_engine: Option<WorkerEngine>,
    agent_control: Option<AgentControl>,
    active_request: Option<PendingRequest>,
    unconfirmed_additions: VecDeque<String>,
    reconnect_attempt: u32,
    turn_failure: Option<String>,
    pending_prompts: VecDeque<PendingPrompt>,
    pending_worker_clarification: Option<PendingWorkerClarification>,
    parallel_turns: HashMap<u64, ParallelTurn>,
    next_turn_id: u64,
    next_worker_label: u64,
    next_message_id: u64,
    last_agent_message: Option<String>,
    turn_had_message: bool,
    listening_requested: bool,
    voice_started_at: Option<Instant>,
    transcription_started_at: Option<Instant>,
    transfer_started_at: Option<Instant>,
    codex_started_at: Option<Instant>,
    acknowledged_turn: Option<u64>,
}

impl Coordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        app: AppHandle,
        shared: Shared,
        settings_path: PathBuf,
        runtime_path: PathBuf,
        settings: AppSettings,
        session_id: Option<String>,
        session_engine: Option<WorkerEngine>,
        pending_request: Option<PendingRequest>,
        command_rx: mpsc::UnboundedReceiver<CoordinatorCommand>,
        speech: SpeechHandle,
        speech_rx: mpsc::UnboundedReceiver<SpeechEngineEvent>,
        agent_rx: mpsc::UnboundedReceiver<AgentEvent>,
        agent_tx: mpsc::UnboundedSender<AgentEvent>,
    ) -> Self {
        let (timed_tx, timed_rx) = mpsc::unbounded_channel();
        let active_engine = pending_request.as_ref().and(session_engine);
        Self {
            app,
            shared,
            settings_path,
            runtime_path,
            settings,
            session_id,
            session_engine,
            command_rx,
            speech,
            speech_rx,
            agent_rx,
            agent_tx,
            timed_rx,
            timed_tx,
            active_generation: 0,
            transcript_revision: 0,
            latest_transcript: String::new(),
            armed_until: None,
            armed_engine: None,
            speaking: false,
            interruption: InterruptionState::None,
            cancellation: CancellationState::Idle,
            voice_suppressed_turn: None,
            parallel_voice: ParallelVoiceState::Enabled,
            active_turn: None,
            active_engine,
            agent_control: None,
            active_request: pending_request,
            unconfirmed_additions: VecDeque::new(),
            reconnect_attempt: 0,
            turn_failure: None,
            pending_prompts: VecDeque::new(),
            pending_worker_clarification: None,
            parallel_turns: HashMap::new(),
            next_turn_id: 1,
            next_worker_label: 2,
            next_message_id: 1,
            last_agent_message: None,
            turn_had_message: false,
            listening_requested: false,
            voice_started_at: None,
            transcription_started_at: None,
            transfer_started_at: None,
            codex_started_at: None,
            acknowledged_turn: None,
        }
    }

    pub async fn run(mut self) {
        self.update_view(|view| {
            view.session_id.clone_from(&self.session_id);
            view.status = "Loading local voice model".into();
            view.pipeline_stage = PipelineStage::Loading;
            view.pipeline_detail = "Loading Whisper large-v3-turbo on the Apple Silicon GPU".into();
        });
        warm_connection(self.settings.remote_config());
        if self.settings.auto_listen {
            self.start_listening();
        }
        if self.active_request.is_some() {
            self.restore_pending_request();
        }

        loop {
            tokio::select! {
                Some(command) = self.command_rx.recv() => self.handle_command(command),
                Some(event) = self.speech_rx.recv() => self.handle_speech_event(event),
                Some(event) = self.agent_rx.recv() => self.handle_agent_event(event),
                Some(event) = self.timed_rx.recv() => self.handle_timed_event(event),
                else => break,
            }
        }
        self.speech.send(SpeechCommand::Shutdown);
    }

    fn handle_command(&mut self, command: CoordinatorCommand) {
        match command {
            CoordinatorCommand::StartListening => self.start_listening(),
            CoordinatorCommand::StopListening => {
                self.commit_interruption();
                self.listening_requested = false;
                self.speech.send(SpeechCommand::StopListening);
                self.update_view(|view| {
                    view.listening = false;
                    view.status = "Listening paused".into();
                    view.pipeline_stage = PipelineStage::Paused;
                    view.pipeline_detail = "Microphone capture is paused".into();
                    view.audio_level = 0.0;
                });
            }
            CoordinatorCommand::SubmitPrompt(prompt) => {
                self.voice_started_at = Some(Instant::now());
                self.submit_prompt(&prompt);
            }
            CoordinatorCommand::SettingsUpdated(settings) => {
                let settings = *settings;
                let remote_changed = self.settings.remote_config() != settings.remote_config()
                    || self.settings.deep_relay != settings.deep_relay
                    || self.settings.local_codex_executable != settings.local_codex_executable
                    || self.settings.reporter_model != settings.reporter_model;
                let voice_input_changed = self.settings.locale != settings.locale
                    || self.settings.wake_phrase != settings.wake_phrase
                    || self.settings.silence_ms != settings.silence_ms
                    || self.settings.voice_isolation != settings.voice_isolation;
                self.settings = settings;
                if remote_changed {
                    self.cancel_active_turn(false);
                    self.clear_session();
                    self.add_system_message(
                        "Worker route or profile settings changed; started a fresh session.",
                    );
                    warm_connection(self.settings.remote_config());
                }
                if self.settings.auto_listen && voice_input_changed && self.listening_requested {
                    self.restart_listening();
                } else if self.settings.auto_listen {
                    self.start_listening();
                } else if !self.settings.auto_listen {
                    self.listening_requested = false;
                    self.speech.send(SpeechCommand::StopListening);
                }
            }
            CoordinatorCommand::NewSession => self.new_session(),
            CoordinatorCommand::CancelTurn => self.request_cancel(true),
            CoordinatorCommand::StopSpeaking => self.stop_voice(),
            CoordinatorCommand::RepeatLast => self.repeat_last(),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn handle_speech_event(&mut self, event: SpeechEngineEvent) {
        match event {
            SpeechEngineEvent::ModelLoading => {
                self.update_view(|view| {
                    view.status = "Loading local Whisper".into();
                    view.pipeline_stage = PipelineStage::Loading;
                    view.pipeline_detail =
                        "Loading large-v3-turbo into Apple Silicon unified memory".into();
                });
            }
            SpeechEngineEvent::ModelReady => {
                self.update_view(|view| {
                    view.pipeline_detail = "Whisper ready • Metal GPU • flash attention".into();
                });
            }
            SpeechEngineEvent::VoiceModelLoading(detail) => {
                self.emit_diagnostic(detail.clone());
                self.update_view(|view| {
                    if matches!(view.pipeline_stage, PipelineStage::Loading) {
                        view.pipeline_detail = format!("Whisper + {detail}");
                    }
                });
            }
            SpeechEngineEvent::VoiceModelReady(detail) => {
                self.emit_diagnostic(detail);
                self.update_view(|view| {
                    if matches!(
                        view.pipeline_stage,
                        PipelineStage::Loading | PipelineStage::Listening | PipelineStage::Paused
                    ) {
                        view.pipeline_detail = if view.listening {
                            "Microphone live • Whisper ready • Kokoro Sarah ready".into()
                        } else {
                            "Voice pipeline ready • Kokoro Sarah warmed".into()
                        };
                    }
                });
            }
            SpeechEngineEvent::VoiceGenerating { tokens, elapsed_ms } => {
                self.update_view(|view| {
                    view.status = "Preparing neural voice".into();
                    view.pipeline_stage = PipelineStage::Responding;
                    view.pipeline_detail = if tokens == 0 {
                        "Kokoro Sarah generating locally on the Apple Silicon GPU".into()
                    } else {
                        format!(
                            "Kokoro Sarah • {tokens} audio tokens • {}.{} s",
                            elapsed_ms / 1_000,
                            (elapsed_ms % 1_000) / 100
                        )
                    };
                });
            }
            SpeechEngineEvent::Authorization(status) => {
                self.emit_diagnostic(status);
            }
            SpeechEngineEvent::MicrophoneRecovering(detail) => {
                self.update_view(|view| {
                    view.listening = true;
                    view.last_error = None;
                    view.status = "Switching microphone route".into();
                    view.pipeline_stage = PipelineStage::Loading;
                    view.pipeline_detail = detail;
                    view.audio_level = 0.0;
                });
            }
            SpeechEngineEvent::ListeningStarted(generation) => {
                self.listening_requested = true;
                self.active_generation = generation;
                self.latest_transcript.clear();
                self.transcript_revision = self.transcript_revision.wrapping_add(1);
                self.update_view(|view| {
                    view.listening = true;
                    view.last_error = None;
                    if view.connectivity == ConnectivityState::Reconnecting {
                        view.status = "Worker link interrupted — reconnecting".into();
                        view.pipeline_stage = PipelineStage::Reconnecting;
                        view.pipeline_detail = format!(
                            "Microphone live • request saved • reconnect attempt {} pending",
                            view.reconnect_attempt
                        );
                    } else if !view.working && !view.speaking {
                        view.status = "Listening for “Hey Codex” or “Hey Claude”".into();
                        view.pipeline_stage = PipelineStage::Listening;
                        view.pipeline_detail =
                            "Microphone live • waiting for speech • Whisper ready".into();
                    } else if !view.working {
                        view.pipeline_stage = PipelineStage::Listening;
                        view.pipeline_detail =
                            "Microphone live • waiting for speech • Whisper ready".into();
                    }
                    view.audio_level = 0.0;
                });
            }
            SpeechEngineEvent::ListeningStopped => {
                self.listening_requested = false;
                self.update_view(|view| {
                    view.listening = false;
                    view.live_transcript.clear();
                    view.pipeline_stage = PipelineStage::Paused;
                    view.pipeline_detail = "Microphone capture is paused".into();
                    view.audio_level = 0.0;
                });
            }
            SpeechEngineEvent::AudioLevel { generation, level }
                if generation == self.active_generation =>
            {
                self.update_view(|view| view.audio_level = level);
            }
            SpeechEngineEvent::SpeechDetected { generation, source }
                if generation == self.active_generation =>
            {
                self.voice_started_at = Some(Instant::now());
                let during_playback = source == DetectedSpeechSource::UserOverRoadiePlayback;
                let interrupting_playback = should_pause_for_detected_speech(
                    during_playback,
                    self.speaking,
                    self.interruption.is_active(),
                );
                if interrupting_playback {
                    self.interruption = InterruptionState::Capturing;
                    self.speech.send(SpeechCommand::PauseSpeaking);
                }
                self.update_view(|view| {
                    view.status = if interrupting_playback {
                        "Voice paused — listening to you".into()
                    } else {
                        "Catching your request".into()
                    };
                    view.pipeline_stage = PipelineStage::Capturing;
                    view.pipeline_detail = if interrupting_playback {
                        "Nearby user voice separated from Roadie playback • paused immediately • capturing directly"
                            .into()
                    } else {
                        "Speech detected • buffering the full utterance locally".into()
                    };
                });
            }
            SpeechEngineEvent::Transcribing {
                generation,
                audio_ms,
            } if generation == self.active_generation => {
                let now = Instant::now();
                let capture_ms = self
                    .voice_started_at
                    .map_or(0, |started| elapsed_ms(started, now));
                self.transcription_started_at = Some(now);
                self.update_view(|view| {
                    view.status = "Whisper is transcribing".into();
                    view.pipeline_stage = PipelineStage::Transcribing;
                    view.pipeline_detail = format!(
                        "Captured in {capture_ms} ms • {}.{}s audio • Apple Silicon Metal",
                        audio_ms / 1_000,
                        (audio_ms % 1_000) / 100
                    );
                    view.audio_level = 0.0;
                });
            }
            SpeechEngineEvent::Transcript {
                generation,
                transcript,
                is_final,
            } => self.handle_transcript(generation, transcript, is_final),
            SpeechEngineEvent::UtteranceIgnored(generation)
                if generation == self.active_generation =>
            {
                if self.interruption.is_active() {
                    self.commit_interruption();
                    self.update_view(|view| {
                        view.status = "Voice stopped — listening for your request".into();
                        view.pipeline_stage = PipelineStage::Listening;
                        view.pipeline_detail =
                            "Playback will not resume over you • no transcript was committed"
                                .into();
                    });
                }
            }
            SpeechEngineEvent::AudioLevel { .. }
            | SpeechEngineEvent::SpeechDetected { .. }
            | SpeechEngineEvent::Transcribing { .. }
            | SpeechEngineEvent::UtteranceIgnored(_) => {}
            SpeechEngineEvent::SpeakingStarted => {
                self.speaking = true;
                self.update_view(|view| {
                    view.speaking = true;
                    view.status = "Speaking".into();
                    if !view.working && !self.interruption.is_active() {
                        view.pipeline_stage = PipelineStage::Responding;
                        view.pipeline_detail = "Playing worker response locally".into();
                    }
                });
            }
            SpeechEngineEvent::SpeakingPaused => {
                self.speaking = false;
                if self.interruption.is_active() {
                    self.interruption = InterruptionState::Paused;
                }
                self.update_view(|view| {
                    view.speaking = false;
                    view.status = if self.interruption.is_active() {
                        "Listening to your interruption".into()
                    } else {
                        "Checking nearby voice".into()
                    };
                    view.pipeline_stage = PipelineStage::Capturing;
                    view.pipeline_detail = if self.interruption.is_active() {
                        "Voice paused immediately • waiting for the rest of your request".into()
                    } else {
                        "Playback ducked • verifying speech after the speaker echo stops".into()
                    };
                });
            }
            SpeechEngineEvent::SpeakingResumed => {
                self.speaking = true;
                self.interruption = InterruptionState::None;
                self.update_view(|view| {
                    view.speaking = true;
                    view.status = "Resuming voice".into();
                    view.pipeline_stage = PipelineStage::Responding;
                    view.pipeline_detail =
                        "Probe matched Roadie's own echo • continuing playback".into();
                });
            }
            SpeechEngineEvent::SpeakingFinished | SpeechEngineEvent::SpeakingCancelled => {
                self.speaking = false;
                if self.interruption == InterruptionState::Paused {
                    self.interruption = InterruptionState::Capturing;
                }
                self.update_view(|view| {
                    view.speaking = false;
                    view.status = if view.connectivity == ConnectivityState::Reconnecting {
                        "Worker link interrupted — reconnecting".into()
                    } else if self.interruption.is_active() {
                        "Listening to your interruption".into()
                    } else if view.working {
                        "Worker is working".into()
                    } else if view.listening {
                        "Listening for “Hey Codex” or “Hey Claude”".into()
                    } else {
                        "Ready".into()
                    };
                    if !view.working && !self.interruption.is_active() {
                        view.pipeline_stage = if view.listening {
                            PipelineStage::Listening
                        } else {
                            PipelineStage::Paused
                        };
                        view.pipeline_detail = if view.listening {
                            "Microphone live • waiting for speech • Whisper ready".into()
                        } else {
                            "Voice pipeline is idle".into()
                        };
                    }
                });
            }
            SpeechEngineEvent::Error(message) => {
                self.commit_interruption();
                self.listening_requested = false;
                self.update_view(|view| {
                    view.listening = false;
                    view.last_error = Some(message.clone());
                    view.status = "Voice error".into();
                    view.pipeline_stage = PipelineStage::Error;
                    view.pipeline_detail.clone_from(&message);
                    view.audio_level = 0.0;
                });
                self.emit_diagnostic(message);
            }
        }
    }

    fn handle_transcript(&mut self, generation: u64, transcript: String, is_final: bool) {
        if generation < self.active_generation {
            return;
        }
        if transcript.trim().is_empty() {
            self.commit_interruption();
            return;
        }
        self.active_generation = generation;
        if is_final && transcript.to_lowercase().starts_with("error:") {
            self.listening_requested = false;
            self.speech.send(SpeechCommand::StopListening);
            self.update_view(|view| {
                view.listening = false;
                view.last_error = Some(transcript.clone());
                view.status = "Voice error".into();
            });
            self.emit_diagnostic(transcript);
            return;
        }
        self.latest_transcript.clone_from(&transcript);
        self.transcript_revision = self.transcript_revision.wrapping_add(1);
        let revision = self.transcript_revision;
        let whisper_ms = self
            .transcription_started_at
            .map(|started| elapsed_ms(started, Instant::now()));
        self.update_view(|view| {
            view.live_transcript = transcript;
            if is_final {
                view.pipeline_detail = whisper_ms.map_or_else(
                    || "Whisper transcript ready • routing wake phrase".into(),
                    |ms| format!("Whisper finished in {ms} ms • routing wake phrase"),
                );
            }
        });
        let delay = if is_final {
            Duration::from_millis(40)
        } else {
            Duration::from_millis(self.settings.silence_ms)
        };
        let sender = self.timed_tx.clone();
        tauri::async_runtime::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = sender.send(TimedEvent::Finalize {
                generation,
                revision,
            });
        });
    }

    fn handle_timed_event(&mut self, event: TimedEvent) {
        match event {
            TimedEvent::Finalize {
                generation,
                revision,
            } if generation == self.active_generation && revision == self.transcript_revision => {
                self.finalize_utterance();
            }
            TimedEvent::Reconnect { turn_id, attempt }
                if self.active_turn == Some(turn_id)
                    && self.agent_control.is_none()
                    && self.active_request.is_some()
                    && self.reconnect_attempt == attempt =>
            {
                self.start_reconnect(turn_id);
            }
            TimedEvent::ParallelReconnect { turn_id, attempt }
                if self.parallel_turns.get(&turn_id).is_some_and(|turn| {
                    turn.control.is_none() && turn.reconnect_attempt == attempt
                }) =>
            {
                self.start_parallel_reconnect(turn_id);
            }
            TimedEvent::Finalize { .. }
            | TimedEvent::Reconnect { .. }
            | TimedEvent::ParallelReconnect { .. } => {}
        }
    }

    fn finalize_utterance(&mut self) {
        let transcript = std::mem::take(&mut self.latest_transcript);
        if transcript.trim().is_empty() {
            self.commit_interruption();
            return;
        }
        self.update_view(|view| view.live_transcript.clear());
        if self.settings.auto_listen {
            self.restart_listening();
        }

        let armed = self
            .armed_until
            .is_some_and(|deadline| deadline > Instant::now());
        let routed = parse_routed_voice_intent(&transcript, &self.settings.wake_phrase, armed);
        let requested_engine = routed
            .engine
            .or_else(|| armed.then_some(self.armed_engine).flatten());
        let intent =
            direct_barge_in_intent(&transcript, routed.intent, self.interruption.is_active());

        if self.interruption.is_active() {
            self.commit_interruption();
        } else if self.speaking
            && !matches!(intent, VoiceIntent::Cancel | VoiceIntent::StopSpeaking)
        {
            return;
        }

        match intent {
            VoiceIntent::NotAddressed => {}
            VoiceIntent::WakeOnly => {
                let engine = requested_engine.unwrap_or_else(|| self.default_engine());
                self.armed_until = Some(Instant::now() + ARMED_WINDOW);
                self.armed_engine = Some(engine);
                self.voice_suppressed_turn = None;
                self.parallel_voice = ParallelVoiceState::Enabled;
                self.speak_local(match engine {
                    WorkerEngine::Claude => "Claude is ready. What would you like me to do?",
                    WorkerEngine::Codex if self.settings.local_worker => {
                        "Codex is ready. What would you like me to do?"
                    }
                    WorkerEngine::Codex => {
                        "The SSH Codex worker is ready. What would you like it to do?"
                    }
                });
            }
            VoiceIntent::Prompt(prompt) => {
                self.armed_until = None;
                self.armed_engine = None;
                self.submit_prompt_to(&prompt, requested_engine);
            }
            VoiceIntent::Cancel => {
                self.armed_until = None;
                self.armed_engine = None;
                self.request_cancel(true);
            }
            VoiceIntent::StopSpeaking => {
                self.armed_until = None;
                self.armed_engine = None;
                self.stop_voice();
            }
            VoiceIntent::NewSession => {
                self.armed_until = None;
                self.armed_engine = None;
                self.new_session_for(requested_engine);
            }
            VoiceIntent::Repeat => {
                self.armed_until = None;
                self.armed_engine = None;
                self.voice_suppressed_turn = None;
                self.parallel_voice = ParallelVoiceState::Enabled;
                self.repeat_last();
            }
            VoiceIntent::Status => {
                self.armed_until = None;
                self.armed_engine = None;
                self.voice_suppressed_turn = None;
                self.parallel_voice = ParallelVoiceState::Enabled;
                self.speak_status_for(requested_engine);
            }
            VoiceIntent::SetMacWorkdir(workspace) => {
                self.armed_until = None;
                self.armed_engine = None;
                self.set_mac_workdir(&workspace);
            }
        }
    }

    fn commit_interruption(&mut self) {
        if self.interruption.is_active() {
            self.interruption = InterruptionState::None;
            self.speech.send(SpeechCommand::StopSpeaking);
        }
    }

    #[allow(clippy::too_many_lines)]
    fn handle_agent_event(&mut self, event: AgentEvent) {
        if self
            .parallel_turns
            .contains_key(&agent_event_turn_id(&event))
        {
            self.handle_parallel_agent_event(event);
            return;
        }
        match event {
            AgentEvent::Transfer { turn_id, detail } if self.active_turn == Some(turn_id) => {
                let transfer_ms = self
                    .transfer_started_at
                    .map(|started| elapsed_ms(started, Instant::now()));
                let prompt_transferred = detail.starts_with("Transferred");
                self.update_view(|view| {
                    view.pipeline_stage = PipelineStage::Transferring;
                    view.pipeline_detail =
                        transfer_ms.map_or(detail.clone(), |ms| format!("{detail} • {ms} ms"));
                });
                if prompt_transferred && self.acknowledged_turn != Some(turn_id) {
                    self.acknowledged_turn = Some(turn_id);
                }
            }
            AgentEvent::Started { turn_id } if self.active_turn == Some(turn_id) => {
                let now = Instant::now();
                let startup_ms = self
                    .transfer_started_at
                    .map_or(0, |started| elapsed_ms(started, now));
                self.codex_started_at = Some(now);
                let reconnecting = self.reconnect_attempt > 0;
                let reconnect_attempt = self.reconnect_attempt;
                let local_claude =
                    self.settings.local_worker && self.current_engine() == WorkerEngine::Claude;
                self.update_view(|view| {
                    view.connectivity = if reconnecting {
                        ConnectivityState::Reconnecting
                    } else {
                        ConnectivityState::Online
                    };
                    view.reconnect_attempt = reconnect_attempt;
                    view.status = if self.settings.local_worker {
                        if reconnecting {
                            if local_claude {
                                "Reached the Mac Claude worker — verifying response".into()
                            } else {
                                "Reached the Mac Codex worker — verifying response".into()
                            }
                        } else if local_claude {
                            "The Mac Claude worker is working".into()
                        } else {
                            "The Mac Codex worker is working".into()
                        }
                    } else if reconnecting {
                        "Reached the SSH Codex worker — verifying response".into()
                    } else {
                        "The SSH Codex worker is working".into()
                    };
                    view.pipeline_stage = PipelineStage::Processing;
                    view.pipeline_detail = if reconnecting {
                        format!(
                            "Reconnect attempt {reconnect_attempt} reached the worker • waiting for the first update before declaring recovery"
                        )
                    } else if local_claude {
                        format!(
                            "Mac-local Claude Code accepted the task • {startup_ms} ms after transfer start"
                        )
                    } else if self.settings.local_worker {
                        format!(
                            "Mac-local Codex accepted the task • {startup_ms} ms after transfer start"
                        )
                    } else {
                        format!(
                            "Remote Codex accepted the prompt through SSH • {startup_ms} ms after transfer start"
                        )
                    };
                });
            }
            AgentEvent::Steered { turn_id } if self.active_turn == Some(turn_id) => {
                self.unconfirmed_additions.pop_front();
                self.persist_session();
                self.update_view(|view| {
                    view.status = "Worker accepted your addition".into();
                    view.pipeline_stage = PipelineStage::Processing;
                    view.pipeline_detail =
                        "Added to the active turn without restarting its work".into();
                });
            }
            AgentEvent::SteerFailed { turn_id, message } if self.active_turn == Some(turn_id) => {
                let mut queued_follow_up = false;
                if let Some(addition) = self.unconfirmed_additions.pop_front() {
                    if is_transient_connectivity_failure(&message) {
                        if let Some(request) = self.active_request.as_mut() {
                            request.additions.push(addition);
                        }
                    } else {
                        let engine = self.current_engine();
                        self.pending_prompts.push_back(PendingPrompt {
                            text: addition,
                            engine,
                        });
                        queued_follow_up = true;
                    }
                    self.persist_session();
                }
                if queued_follow_up {
                    self.add_system_message(
                        "The active thread could not accept that addition live, so Roadie queued it for the same primary session after the current turn.",
                    );
                    self.update_view(|view| {
                        view.status = "Addition queued for the primary worker".into();
                        view.pipeline_detail =
                            "The current turn keeps working • addition runs next in the same session"
                                .into();
                    });
                    self.speak_local("That addition is queued for the primary worker.");
                } else {
                    self.emit_diagnostic(format!(
                        "Could not add to the active turn; saved for reconnect: {message}"
                    ));
                    self.speak_local("That addition is saved for reconnect.");
                }
            }
            AgentEvent::ThreadStarted {
                turn_id,
                session_id,
            } if self.active_turn == Some(turn_id) => {
                self.session_id = Some(session_id.clone());
                self.session_engine = Some(self.current_engine());
                self.update_view(|view| view.session_id = Some(session_id));
                let local_claude =
                    self.settings.local_worker && self.current_engine() == WorkerEngine::Claude;
                self.update_view(|view| {
                    view.pipeline_stage = PipelineStage::Processing;
                    view.pipeline_detail = if local_claude && self.settings.deep_relay {
                        "Claude Code session connected • local reporter standing by".into()
                    } else if local_claude {
                        "Claude Code session connected".into()
                    } else if self.settings.local_worker && self.settings.deep_relay {
                        "Mac Codex thread connected • local reporter standing by".into()
                    } else if self.settings.local_worker {
                        "Mac Codex thread connected".into()
                    } else if self.settings.deep_relay {
                        "SSH Codex thread connected • local reporter standing by".into()
                    } else {
                        "SSH Codex thread connected".into()
                    };
                });
                self.persist_session();
            }
            AgentEvent::ReporterStatus { turn_id, detail } if self.active_turn == Some(turn_id) => {
                self.update_view(|view| {
                    view.status = "Local voice agent reporting".into();
                    view.pipeline_stage = PipelineStage::Reporting;
                    view.pipeline_detail = detail;
                });
            }
            AgentEvent::WorkerMessage { turn_id, text } if self.active_turn == Some(turn_id) => {
                self.confirm_reconnected();
                let now = Instant::now();
                let codex_ms = self
                    .codex_started_at
                    .map_or(0, |started| elapsed_ms(started, now));
                let total_ms = self
                    .voice_started_at
                    .map(|started| elapsed_ms(started, now));
                self.turn_had_message = true;
                let local_claude =
                    self.settings.local_worker && self.current_engine() == WorkerEngine::Claude;
                let worker_name = if local_claude { "Claude Code" } else { "Codex" };
                if self.settings.deep_relay {
                    self.add_message(ChatRole::Worker, text);
                    self.update_view(|view| {
                        view.pipeline_stage = PipelineStage::Reporting;
                        view.pipeline_detail = total_ms.map_or_else(
                            || format!("{worker_name} update in {codex_ms} ms • queued to local Spark"),
                            |total| {
                                format!(
                                    "{worker_name} update in {codex_ms} ms • {total} ms total • local report queued"
                                )
                            },
                        );
                    });
                } else {
                    let voiced_text =
                        attributed_execution_update(self.settings.local_worker, &text);
                    self.last_agent_message = Some(voiced_text.clone());
                    self.add_message(ChatRole::Agent, voiced_text.clone());
                    self.update_view(|view| {
                        view.pipeline_stage = PipelineStage::Responding;
                        view.pipeline_detail = total_ms.map_or_else(
                            || format!("{worker_name} response in {codex_ms} ms • preparing playback"),
                            |total| {
                                format!(
                                    "{worker_name} response in {codex_ms} ms • {total} ms voice-to-response"
                                )
                            },
                        );
                    });
                    self.speak_local(&voiced_text);
                }
            }
            AgentEvent::VoiceMessage {
                turn_id,
                text,
                model,
                fallback,
            } if self.active_turn == Some(turn_id) => {
                self.last_agent_message = Some(text.clone());
                self.add_message(ChatRole::Reporter, text.clone());
                self.update_view(|view| {
                    view.pipeline_stage = PipelineStage::Responding;
                    view.pipeline_detail = if fallback {
                        format!("Speaking via {model} fallback")
                    } else {
                        format!("Speaking local {model} report")
                    };
                });
                self.speak_local(&text);
            }
            AgentEvent::Usage {
                turn_id,
                lane,
                usage,
            } if self.active_turn == Some(turn_id) => {
                let local_claude =
                    self.settings.local_worker && self.current_engine() == WorkerEngine::Claude;
                self.update_view(|view| match lane {
                    AgentLane::Worker => {
                        view.last_usage = Some(usage);
                        view.session_usage.accumulate(usage);
                        let worker = if local_claude { "Claude Code" } else { "Codex" };
                        view.pipeline_detail = format!(
                            "{worker} complete • {} input • {} output • finishing local reports",
                            usage.input_tokens, usage.output_tokens
                        );
                    }
                    AgentLane::Reporter => {
                        view.last_reporter_usage
                            .get_or_insert_with(Default::default)
                            .accumulate(usage);
                        view.session_reporter_usage.accumulate(usage);
                    }
                });
                self.persist_session();
            }
            AgentEvent::TurnCommitted { turn_id } if self.active_turn == Some(turn_id) => {
                self.reconnect_attempt = 0;
                self.active_request = None;
                let engine = self.current_engine();
                while let Some(addition) = self.unconfirmed_additions.pop_front() {
                    self.pending_prompts.push_back(PendingPrompt {
                        text: addition,
                        engine,
                    });
                }
                self.persist_session();
                self.emit_diagnostic(
                    "Worker turn committed; cleared the durable pending request before process teardown"
                        .into(),
                );
            }
            AgentEvent::Failed { turn_id, message } if self.active_turn == Some(turn_id) => {
                if is_transient_connectivity_failure(&message) {
                    self.turn_failure = Some(message.clone());
                    self.emit_diagnostic(format!("Recoverable worker-link failure: {message}"));
                } else {
                    self.turn_had_message = true;
                    self.turn_failure = Some(message.clone());
                    self.add_system_message(&format!("Worker error: {message}"));
                    self.speak_local(&format!("The worker reported an error. {message}"));
                }
            }
            AgentEvent::Diagnostic { turn_id, message } if self.active_turn == Some(turn_id) => {
                self.emit_diagnostic(message);
            }
            AgentEvent::Exited {
                turn_id,
                success,
                cancelled,
                stderr,
            } if self.active_turn == Some(turn_id) => {
                let finished_engine = self.current_engine();
                self.agent_control = None;
                let failure = self.turn_failure.take();
                let detail = failure
                    .as_deref()
                    .filter(|message| !message.trim().is_empty())
                    .map_or_else(
                        || concise_error(&stderr),
                        |message| message.trim().to_owned(),
                    );
                let connectivity_lost = !success
                    && !cancelled
                    && (is_transient_connectivity_failure(&detail)
                        || is_transient_connectivity_failure(&stderr));

                if connectivity_lost {
                    while let Some(addition) = self.unconfirmed_additions.pop_front() {
                        if let Some(request) = self.active_request.as_mut() {
                            request.additions.push(addition);
                        }
                    }
                    self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
                    let delay = reconnect_delay(self.reconnect_attempt);
                    let queued = self
                        .active_request
                        .as_ref()
                        .map_or(0, |request| request.additions.len());
                    let worker_count = self.total_active_workers();
                    self.update_view(|view| {
                        view.working = true;
                        view.active_workers = worker_count;
                        view.connectivity = ConnectivityState::Reconnecting;
                        view.reconnect_attempt = self.reconnect_attempt;
                        view.queued_additions = queued;
                        view.status = "Worker link interrupted — reconnecting".into();
                        view.pipeline_stage = PipelineStage::Reconnecting;
                        view.pipeline_detail = format!(
                            "Request saved locally • retry {} in {}s • voice stays available",
                            self.reconnect_attempt,
                            delay.as_secs()
                        );
                    });
                    self.persist_session();
                    self.emit_diagnostic(format!(
                        "Worker link interrupted; reconnect attempt {} scheduled in {}s: {detail}",
                        self.reconnect_attempt,
                        delay.as_secs()
                    ));
                    if self.reconnect_attempt == 1 {
                        self.add_system_message(
                            "The worker link was interrupted. The active request is saved locally and will resume automatically.",
                        );
                        self.speak_local(
                            "The worker link was interrupted. Roadie saved your request and will reconnect automatically.",
                        );
                    }
                    self.schedule_reconnect(turn_id, self.reconnect_attempt, delay);
                } else {
                    self.active_turn = None;
                    self.active_engine = None;
                    self.cancellation = CancellationState::Idle;
                    self.voice_suppressed_turn = None;
                    self.active_request = None;
                    self.unconfirmed_additions.clear();
                    self.reconnect_attempt = 0;
                    self.voice_started_at = None;
                    self.transcription_started_at = None;
                    self.transfer_started_at = None;
                    self.codex_started_at = None;
                    self.acknowledged_turn = None;
                    let remaining_workers = self.parallel_turns.len();
                    let parallel_reconnecting = self.any_worker_reconnecting();
                    self.update_view(|view| {
                        view.working = remaining_workers > 0;
                        view.active_workers = remaining_workers;
                        view.connectivity = if parallel_reconnecting {
                            ConnectivityState::Reconnecting
                        } else {
                            ConnectivityState::Online
                        };
                        view.reconnect_attempt = 0;
                        view.queued_additions = 0;
                        view.status = if parallel_reconnecting {
                            "Parallel worker reconnecting".into()
                        } else if remaining_workers > 0 {
                            format!("{remaining_workers} parallel worker(s) still working")
                        } else if view.speaking {
                            "Speaking".into()
                        } else if view.listening {
                            "Listening for “Hey Codex” or “Hey Claude”".into()
                        } else {
                            "Ready".into()
                        };
                        if parallel_reconnecting {
                            view.pipeline_stage = PipelineStage::Reconnecting;
                            view.pipeline_detail = format!(
                                "{remaining_workers} independent worker session(s) retained"
                            );
                        } else if remaining_workers > 0 {
                            view.pipeline_stage = PipelineStage::Processing;
                            view.pipeline_detail = format!(
                                "{remaining_workers} independent worker session(s) still active"
                            );
                        } else if view.listening {
                            view.pipeline_stage = PipelineStage::Listening;
                            view.pipeline_detail =
                                "Microphone live • waiting for speech • Whisper ready".into();
                        } else {
                            view.pipeline_stage = PipelineStage::Paused;
                            view.pipeline_detail = "Voice pipeline is idle".into();
                        }
                    });
                    self.persist_session();
                    if cancelled {
                        self.add_system_message("Worker turn cancelled.");
                    } else if !success {
                        if self.turn_had_message {
                            self.emit_diagnostic(format!(
                                "Worker process exited unsuccessfully: {detail}"
                            ));
                        } else if self.settings.remote_config_for_engine(finished_engine).host
                            == WorkerHost::RemoteSsh
                            && is_ssh_transport_failure(&stderr)
                        {
                            self.add_system_message(&format!("SSH transport failed: {detail}"));
                            self.speak_local(&format!("The SSH transport failed. {detail}"));
                        } else if self.settings.local_worker {
                            let worker = match finished_engine {
                                WorkerEngine::Claude => "Claude Code",
                                WorkerEngine::Codex => "Codex",
                            };
                            self.add_system_message(&format!(
                                "Mac-local {worker} failed: {detail}"
                            ));
                            self.speak_local(&format!(
                                "The Mac {worker} worker stopped before responding. {detail}"
                            ));
                        } else {
                            self.add_system_message(&format!("Remote Codex failed: {detail}"));
                            self.speak_local(&format!(
                                "Remote Codex stopped before responding. {detail}"
                            ));
                        }
                    } else if !self.turn_had_message {
                        self.speak_local("The worker finished without a chat response.");
                    }
                    if let Some(prompt) = self.pending_prompts.pop_front() {
                        self.submit_prompt_to(&prompt.text, Some(prompt.engine));
                    }
                }
            }
            _ => {}
        }
    }

    #[allow(clippy::too_many_lines)]
    fn handle_parallel_agent_event(&mut self, event: AgentEvent) {
        let turn_id = agent_event_turn_id(&event);
        let Some(label) = self.parallel_turns.get(&turn_id).map(|turn| turn.label) else {
            return;
        };
        match event {
            AgentEvent::Transfer { detail, .. } => {
                self.update_view(|view| {
                    view.status = format!("Transferring to worker {label}");
                    view.pipeline_stage = PipelineStage::Transferring;
                    view.pipeline_detail = format!("Worker {label} • {detail}");
                });
            }
            AgentEvent::Started { .. } => {
                self.update_view(|view| {
                    view.status = format!("Worker {label} is working");
                    view.pipeline_stage = PipelineStage::Processing;
                    view.pipeline_detail = format!(
                        "{} workers active • worker {label} accepted its independent task",
                        self.total_active_workers()
                    );
                });
            }
            AgentEvent::ThreadStarted { session_id, .. } => {
                if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                    turn.session_id = Some(session_id);
                }
            }
            AgentEvent::WorkerMessage { text, .. } => {
                if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                    turn.had_message = true;
                    turn.failure = None;
                    turn.reconnect_attempt = 0;
                }
                let any_reconnecting = self.any_worker_reconnecting();
                let attributed = format!("Worker {label}: {text}");
                if self.settings.deep_relay {
                    self.add_message(ChatRole::Worker, attributed);
                } else {
                    self.last_agent_message = Some(attributed.clone());
                    self.add_message(ChatRole::Agent, attributed.clone());
                    self.speak_local(&attributed);
                }
                self.update_view(|view| {
                    view.connectivity = if any_reconnecting {
                        ConnectivityState::Reconnecting
                    } else {
                        ConnectivityState::Online
                    };
                    view.status = format!("Worker {label} reported");
                    view.pipeline_stage = if self.settings.deep_relay {
                        PipelineStage::Reporting
                    } else {
                        PipelineStage::Responding
                    };
                    view.pipeline_detail = format!(
                        "{} parallel worker session(s) active",
                        self.total_active_workers()
                    );
                });
            }
            AgentEvent::VoiceMessage {
                text,
                model,
                fallback,
                ..
            } => {
                let attributed = format!("Worker {label}: {text}");
                self.last_agent_message = Some(attributed.clone());
                self.add_message(ChatRole::Reporter, attributed.clone());
                self.update_view(|view| {
                    view.status = format!("Worker {label} voice report");
                    view.pipeline_stage = PipelineStage::Responding;
                    view.pipeline_detail = if fallback {
                        format!("Worker {label} • speaking via {model} fallback")
                    } else {
                        format!("Worker {label} • speaking local {model} report")
                    };
                });
                self.speak_local(&attributed);
            }
            AgentEvent::ReporterStatus { detail, .. } => {
                self.update_view(|view| {
                    view.status = format!("Reporting worker {label}");
                    view.pipeline_stage = PipelineStage::Reporting;
                    view.pipeline_detail = format!("Worker {label} • {detail}");
                });
            }
            AgentEvent::Usage { lane, usage, .. } => {
                self.update_view(|view| match lane {
                    AgentLane::Worker => {
                        view.last_usage = Some(usage);
                        view.session_usage.accumulate(usage);
                    }
                    AgentLane::Reporter => {
                        view.last_reporter_usage
                            .get_or_insert_with(Default::default)
                            .accumulate(usage);
                        view.session_reporter_usage.accumulate(usage);
                    }
                });
            }
            AgentEvent::TurnCommitted { .. } => {
                if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                    turn.committed = true;
                    turn.request.additions.clear();
                }
            }
            AgentEvent::Failed { message, .. } => {
                if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                    turn.failure = Some(message.clone());
                }
                self.emit_diagnostic(format!("Parallel worker {label} failure: {message}"));
            }
            AgentEvent::Diagnostic { message, .. } => {
                self.emit_diagnostic(format!("Worker {label}: {message}"));
            }
            AgentEvent::SteerFailed { message, .. } => {
                if let Some(turn) = self.parallel_turns.get_mut(&turn_id)
                    && let Some(addition) = turn.unconfirmed_additions.pop_front()
                {
                    turn.queued_follow_ups.push(addition);
                }
                self.emit_diagnostic(format!("Worker {label} steering failed: {message}"));
                self.update_view(|view| {
                    view.status = format!("Saved for worker {label}");
                    view.pipeline_detail = format!(
                        "Worker {label} could not accept live steering • addition retained for its session"
                    );
                });
                self.speak_local(&format!(
                    "Worker {label} could not accept that live. I kept it with that worker."
                ));
            }
            AgentEvent::Steered { .. } => {
                if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                    turn.unconfirmed_additions.pop_front();
                }
                self.update_view(|view| {
                    view.status = format!("Worker {label} accepted your addition");
                    view.pipeline_stage = PipelineStage::Processing;
                    view.pipeline_detail =
                        format!("Worker {label} continues without restarting its task");
                });
            }
            AgentEvent::Exited {
                success,
                cancelled,
                stderr,
                ..
            } => {
                let Some((failure, committed, had_message, session_id)) =
                    self.parallel_turns.get(&turn_id).map(|turn| {
                        (
                            turn.failure.clone(),
                            turn.committed,
                            turn.had_message,
                            turn.session_id.clone(),
                        )
                    })
                else {
                    return;
                };
                let detail = failure
                    .as_deref()
                    .filter(|message| !message.trim().is_empty())
                    .map_or_else(|| concise_error(&stderr), str::to_owned);
                let connectivity_lost = !success
                    && !cancelled
                    && !committed
                    && (is_transient_connectivity_failure(&detail)
                        || is_transient_connectivity_failure(&stderr));
                if connectivity_lost {
                    let attempt = if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                        turn.control = None;
                        turn.reconnect_attempt = turn.reconnect_attempt.saturating_add(1);
                        turn.reconnect_attempt
                    } else {
                        return;
                    };
                    let delay = reconnect_delay(attempt);
                    self.update_view(|view| {
                        view.connectivity = ConnectivityState::Reconnecting;
                        view.status = format!("Worker {label} reconnecting");
                        view.pipeline_stage = PipelineStage::Reconnecting;
                        view.pipeline_detail = format!(
                            "Parallel worker {label} saved • retry {attempt} in {}s",
                            delay.as_secs()
                        );
                    });
                    self.schedule_parallel_reconnect(turn_id, attempt, delay);
                    return;
                }

                let Some(finished_turn) = self.parallel_turns.remove(&turn_id) else {
                    return;
                };
                if !cancelled && !finished_turn.queued_follow_ups.is_empty() {
                    self.continue_parallel_worker(finished_turn);
                    return;
                }
                if self.parallel_turns.is_empty() {
                    self.parallel_voice = ParallelVoiceState::Enabled;
                }
                if !success && !cancelled && !committed {
                    self.add_system_message(&format!("Worker {label} failed: {detail}"));
                    self.speak_local(&format!("Worker {label} stopped before finishing."));
                } else if success && !had_message {
                    self.add_system_message(&format!(
                        "Worker {label} finished without a chat response."
                    ));
                }
                if committed && session_id.is_some() {
                    self.emit_diagnostic(format!(
                        "Parallel worker {label} committed its independent session"
                    ));
                }
                self.refresh_worker_view();
                if self.total_active_workers() < MAX_CONCURRENT_WORKERS
                    && let Some(next) = self.pending_prompts.pop_front()
                {
                    self.launch_parallel_worker(next.text, next.engine);
                }
            }
        }
    }

    fn default_engine(&self) -> WorkerEngine {
        self.settings.remote_config().engine
    }

    fn current_engine(&self) -> WorkerEngine {
        self.active_engine
            .or(self.session_engine)
            .unwrap_or_else(|| self.default_engine())
    }

    fn activate_session_engine(&mut self, engine: WorkerEngine) {
        if self.session_engine == Some(engine) {
            return;
        }
        self.session_id = None;
        self.session_engine = Some(engine);
        self.update_view(|view| {
            view.session_id = None;
            view.last_usage = None;
            view.session_usage = roadie_core::TokenUsage::default();
            view.last_reporter_usage = None;
            view.session_reporter_usage = roadie_core::TokenUsage::default();
        });
    }

    #[allow(
        clippy::too_many_lines,
        reason = "submission owns the atomic persist, UI transition, and app-server launch sequence"
    )]
    fn submit_prompt(&mut self, prompt: &str) {
        self.submit_prompt_to(prompt, None);
    }

    #[allow(
        clippy::too_many_lines,
        reason = "submission owns voice routing, atomic persistence, UI transition, and worker launch"
    )]
    fn submit_prompt_to(&mut self, prompt: &str, requested_engine: Option<WorkerEngine>) {
        let prompt = prompt.trim().to_owned();
        if prompt.is_empty() {
            return;
        }
        if self.handle_worker_clarification(&prompt) {
            return;
        }
        let engine = requested_engine.unwrap_or_else(|| self.default_engine());
        if engine == WorkerEngine::Claude && !self.settings.local_worker {
            self.add_system_message(
                "“Hey Claude” requires the Mac-local worker mode; the remote SSH worker supports Codex only.",
            );
            self.speak_local(
                "Claude voice routing requires Mac-local worker mode. I did not send this request to Codex.",
            );
            return;
        }
        if self.cancellation == CancellationState::Requested {
            self.add_message(ChatRole::User, prompt.clone());
            self.pending_prompts.push_back(PendingPrompt {
                text: prompt,
                engine,
            });
            self.update_view(|view| {
                view.status = "Queued after cancellation".into();
                view.pipeline_stage = PipelineStage::Transferring;
                view.pipeline_detail =
                    "Waiting for turn/interrupt confirmation, then starting your next request"
                        .into();
            });
            self.speak_force("Queued next.");
            return;
        }
        if self.total_active_workers() > 0 {
            let active_engine = self.active_engine.unwrap_or_else(|| self.default_engine());
            let (parallel_prompt, force_parallel) = strip_parallel_request_prefix(&prompt);
            let should_parallel = force_parallel
                || (self.active_turn.is_some()
                    && should_launch_parallel(false, requested_engine, active_engine));
            if should_parallel {
                self.add_message(ChatRole::User, prompt.clone());
                self.launch_parallel_worker(parallel_prompt.to_owned(), engine);
                return;
            }
            self.add_message(ChatRole::User, prompt.clone());
            let target = match referenced_worker(&prompt) {
                WorkerReference::Label(label) => self.worker_target_for_label(label),
                WorkerReference::None if self.total_active_workers() == 1 => {
                    self.sole_active_worker_target()
                }
                WorkerReference::None | WorkerReference::Ambiguous => None,
            };
            if let Some(target) = target {
                self.route_addition_to_worker(target, prompt, engine);
            } else {
                self.ask_which_worker(prompt, engine);
            }
            return;
        }
        let config = self.settings.remote_config_for_engine(engine);
        if let Err(error) = config.validate() {
            self.add_system_message(&format!("Setup required: {error}"));
            self.speak_local(
                "The execution worker is not configured yet. Open settings once before driving.",
            );
            return;
        }
        if should_start_fresh_session(&prompt) {
            self.clear_session();
        }
        self.activate_session_engine(engine);
        self.active_engine = Some(engine);

        self.add_message(ChatRole::User, prompt.clone());
        self.next_turn_id = self.next_turn_id.wrapping_add(1);
        let turn_id = self.next_turn_id;
        self.cancellation = CancellationState::Idle;
        self.voice_suppressed_turn = None;
        self.parallel_voice = ParallelVoiceState::Enabled;
        self.active_turn = Some(turn_id);
        self.active_request = Some(PendingRequest {
            prompt: prompt.clone(),
            additions: Vec::new(),
        });
        self.unconfirmed_additions.clear();
        self.reconnect_attempt = 0;
        self.turn_failure = None;
        self.turn_had_message = false;
        self.voice_started_at.get_or_insert_with(Instant::now);
        self.transfer_started_at = Some(Instant::now());
        self.codex_started_at = None;
        self.acknowledged_turn = None;
        let local_claude = self.settings.local_worker && engine == WorkerEngine::Claude;
        self.update_view(|view| {
            view.working = true;
            view.active_workers = 1 + self.parallel_turns.len();
            view.connectivity = ConnectivityState::Online;
            view.reconnect_attempt = 0;
            view.queued_additions = 0;
            view.last_error = None;
            view.last_reporter_usage = None;
            view.status = if local_claude {
                "Sending to Claude Code".into()
            } else {
                "Sending to Codex".into()
            };
            view.pipeline_stage = PipelineStage::Transferring;
            view.pipeline_detail = if local_claude {
                "Starting the Mac-local Claude Code worker".into()
            } else if self.settings.local_worker {
                "Starting the Mac-local Codex worker".into()
            } else {
                "Opening the key-only SSH worker link".into()
            };
        });
        self.persist_session();
        let reporter = self.settings.deep_relay.then(|| ReporterConfig {
            executable: self.settings.local_codex_executable.trim().to_owned(),
            primary_model: self.settings.reporter_model.trim().to_owned(),
            fallback_model: self.settings.reporter_fallback_model().into(),
            worker_host: config.host,
            worker_target: config.ssh_target.clone(),
            worker_workspace: config.workspace.clone(),
        });
        let voice_prompt = execution_worker_prompt(
            &prompt,
            self.settings.local_worker,
            config.engine,
            config.sandbox,
            self.settings.deep_relay,
            &self.settings.ssh_target,
            &self.settings.workspace,
        );
        self.agent_control = Some(spawn_turn(
            turn_id,
            config,
            reporter,
            self.session_id.clone(),
            voice_prompt,
            self.agent_tx.clone(),
        ));
        self.acknowledged_turn = Some(turn_id);
        self.speak_local(if self.settings.local_worker {
            match engine {
                WorkerEngine::Codex => "Sending that to Codex.",
                WorkerEngine::Claude => "Sending that to Claude.",
            }
        } else {
            "Sending that to the SSH worker."
        });
    }

    fn launch_parallel_worker(&mut self, prompt: String, engine: WorkerEngine) {
        if self.total_active_workers() >= MAX_CONCURRENT_WORKERS {
            self.pending_prompts.push_back(PendingPrompt {
                text: prompt,
                engine,
            });
            self.update_view(|view| {
                view.status = "Parallel task queued".into();
                view.pipeline_stage = PipelineStage::Transferring;
                view.pipeline_detail = format!(
                    "{MAX_CONCURRENT_WORKERS} workers active • queued until the next worker slot is free"
                );
            });
            self.speak_local("All worker slots are busy. I queued that task next.");
            return;
        }

        let config = self.settings.remote_config_for_engine(engine);
        if let Err(error) = config.validate() {
            self.add_system_message(&format!("Setup required: {error}"));
            self.speak_local("The parallel worker is not configured yet.");
            return;
        }
        self.next_turn_id = self.next_turn_id.wrapping_add(1);
        let turn_id = self.next_turn_id;
        let label = self.next_worker_label;
        self.next_worker_label = self.next_worker_label.wrapping_add(1);
        let request = PendingRequest {
            prompt: prompt.clone(),
            additions: Vec::new(),
        };
        let reporter = self.settings.deep_relay.then(|| ReporterConfig {
            executable: self.settings.local_codex_executable.trim().to_owned(),
            primary_model: self.settings.reporter_model.trim().to_owned(),
            fallback_model: self.settings.reporter_fallback_model().into(),
            worker_host: config.host,
            worker_target: config.ssh_target.clone(),
            worker_workspace: config.workspace.clone(),
        });
        let worker_prompt = parallel_worker_prompt(
            label,
            &execution_worker_prompt(
                &prompt,
                self.settings.local_worker,
                config.engine,
                config.sandbox,
                self.settings.deep_relay,
                &self.settings.ssh_target,
                &self.settings.workspace,
            ),
        );
        let control = spawn_turn(
            turn_id,
            config,
            reporter,
            None,
            worker_prompt,
            self.agent_tx.clone(),
        );
        self.parallel_turns.insert(
            turn_id,
            ParallelTurn {
                label,
                engine,
                request,
                session_id: None,
                control: Some(control),
                reconnect_attempt: 0,
                failure: None,
                had_message: false,
                committed: false,
                unconfirmed_additions: VecDeque::new(),
                queued_follow_ups: Vec::new(),
            },
        );
        self.parallel_voice = ParallelVoiceState::Enabled;
        let worker_count = self.total_active_workers();
        self.update_view(|view| {
            view.working = true;
            view.active_workers = worker_count;
            view.status = format!("Worker {label} started in parallel");
            view.pipeline_stage = PipelineStage::Transferring;
            view.pipeline_detail = format!(
                "{worker_count} workers active • worker {label} has an independent session"
            );
        });
        self.speak_local(&format!("Started worker {label} in parallel."));
    }

    fn start_parallel_reconnect(&mut self, turn_id: u64) {
        let Some((label, engine, request, session_id, attempt)) =
            self.parallel_turns.get(&turn_id).map(|turn| {
                (
                    turn.label,
                    turn.engine,
                    turn.request.clone(),
                    turn.session_id.clone(),
                    turn.reconnect_attempt,
                )
            })
        else {
            return;
        };
        let config = self.settings.remote_config_for_engine(engine);
        if let Err(error) = config.validate() {
            self.parallel_turns.remove(&turn_id);
            if self.parallel_turns.is_empty() {
                self.parallel_voice = ParallelVoiceState::Enabled;
            }
            self.add_system_message(&format!("Worker {label} could not reconnect: {error}"));
            self.refresh_worker_view();
            return;
        }
        let recovery = reconnect_prompt(&request, session_id.is_some(), config.engine);
        let reporter = self.settings.deep_relay.then(|| ReporterConfig {
            executable: self.settings.local_codex_executable.trim().to_owned(),
            primary_model: self.settings.reporter_model.trim().to_owned(),
            fallback_model: self.settings.reporter_fallback_model().into(),
            worker_host: config.host,
            worker_target: config.ssh_target.clone(),
            worker_workspace: config.workspace.clone(),
        });
        let worker_prompt = parallel_worker_prompt(
            label,
            &execution_worker_prompt(
                &recovery,
                self.settings.local_worker,
                config.engine,
                config.sandbox,
                self.settings.deep_relay,
                &self.settings.ssh_target,
                &self.settings.workspace,
            ),
        );
        let control = spawn_turn(
            turn_id,
            config,
            reporter,
            session_id,
            worker_prompt,
            self.agent_tx.clone(),
        );
        if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
            turn.control = Some(control);
            turn.failure = None;
        }
        self.update_view(|view| {
            view.status = format!("Reconnecting worker {label}");
            view.pipeline_stage = PipelineStage::Reconnecting;
            view.pipeline_detail = format!("Parallel worker {label} • reconnect attempt {attempt}");
        });
    }

    fn continue_parallel_worker(&mut self, previous: ParallelTurn) {
        if previous.queued_follow_ups.is_empty() {
            return;
        }
        let label = previous.label;
        let engine = previous.engine;
        let config = self.settings.remote_config_for_engine(engine);
        if let Err(error) = config.validate() {
            let held = previous.queued_follow_ups.join("\n");
            self.pending_worker_clarification = Some(PendingWorkerClarification {
                prompt: held,
                engine,
            });
            self.add_system_message(&format!(
                "Worker {label} ended before its held addition could resume: {error}. The addition remains held locally."
            ));
            self.speak_local(&format!(
                "Worker {label} could not resume. I kept its addition locally."
            ));
            return;
        }

        self.next_turn_id = self.next_turn_id.wrapping_add(1);
        let turn_id = self.next_turn_id;
        let request = PendingRequest {
            prompt: previous.request.prompt,
            additions: previous.queued_follow_ups,
        };
        let follow_up = parallel_follow_up_prompt(&request, previous.session_id.is_some());
        let reporter = self.settings.deep_relay.then(|| ReporterConfig {
            executable: self.settings.local_codex_executable.trim().to_owned(),
            primary_model: self.settings.reporter_model.trim().to_owned(),
            fallback_model: self.settings.reporter_fallback_model().into(),
            worker_host: config.host,
            worker_target: config.ssh_target.clone(),
            worker_workspace: config.workspace.clone(),
        });
        let worker_prompt = parallel_worker_prompt(
            label,
            &execution_worker_prompt(
                &follow_up,
                self.settings.local_worker,
                config.engine,
                config.sandbox,
                self.settings.deep_relay,
                &self.settings.ssh_target,
                &self.settings.workspace,
            ),
        );
        let control = spawn_turn(
            turn_id,
            config,
            reporter,
            previous.session_id.clone(),
            worker_prompt,
            self.agent_tx.clone(),
        );
        self.parallel_turns.insert(
            turn_id,
            ParallelTurn {
                label,
                engine,
                request,
                session_id: previous.session_id,
                control: Some(control),
                reconnect_attempt: 0,
                failure: None,
                had_message: false,
                committed: false,
                unconfirmed_additions: VecDeque::new(),
                queued_follow_ups: Vec::new(),
            },
        );
        self.parallel_voice = ParallelVoiceState::Enabled;
        self.refresh_worker_view();
        self.update_view(|view| {
            view.status = format!("Continuing worker {label}");
            view.pipeline_stage = PipelineStage::Transferring;
            view.pipeline_detail = format!(
                "Worker {label} ended before live steering completed • resuming its session with the held addition"
            );
        });
        self.speak_local(&format!(
            "Continuing worker {label} with its saved addition."
        ));
    }

    fn total_active_workers(&self) -> usize {
        usize::from(self.active_turn.is_some()) + self.parallel_turns.len()
    }

    fn active_worker_choices(&self) -> Vec<(u64, String)> {
        let mut choices = Vec::new();
        if self.active_turn.is_some() {
            choices.push((
                1,
                self.active_request
                    .as_ref()
                    .map_or_else(|| "primary task".into(), |request| request.prompt.clone()),
            ));
        }
        choices.extend(
            self.parallel_turns
                .values()
                .map(|turn| (turn.label, turn.request.prompt.clone())),
        );
        choices.sort_by_key(|(label, _)| *label);
        choices
    }

    fn worker_target_for_label(&self, label: u64) -> Option<WorkerTarget> {
        if label == 1 && self.active_turn.is_some() {
            return Some(WorkerTarget::Primary);
        }
        self.parallel_turns.iter().find_map(|(turn_id, turn)| {
            (turn.label == label).then_some(WorkerTarget::Parallel(*turn_id))
        })
    }

    fn sole_active_worker_target(&self) -> Option<WorkerTarget> {
        let mut targets = Vec::new();
        if self.active_turn.is_some() {
            targets.push(WorkerTarget::Primary);
        }
        targets.extend(
            self.parallel_turns
                .keys()
                .copied()
                .map(WorkerTarget::Parallel),
        );
        (targets.len() == 1).then_some(targets[0])
    }

    fn ask_which_worker(&mut self, prompt: String, engine: WorkerEngine) {
        self.pending_worker_clarification = Some(PendingWorkerClarification { prompt, engine });
        let question = worker_clarification_question(&self.active_worker_choices());
        self.add_system_message(&question);
        self.update_view(|view| {
            view.status = "Which worker?".into();
            view.pipeline_stage = PipelineStage::Processing;
            view.pipeline_detail =
                "Request held locally • waiting for a worker number • no task was redirected"
                    .into();
        });
        self.speak_local(&question);
    }

    fn handle_worker_clarification(&mut self, response: &str) -> bool {
        let Some(pending) = self.pending_worker_clarification.clone() else {
            return false;
        };
        self.add_message(ChatRole::User, response.to_owned());
        let WorkerReference::Label(label) = clarification_worker_reference(response) else {
            let question = worker_clarification_question(&self.active_worker_choices());
            self.add_system_message(&format!(
                "I still need one worker number before I can route the held request. {question}"
            ));
            self.update_view(|view| {
                view.status = "Worker number needed".into();
                view.pipeline_stage = PipelineStage::Processing;
                view.pipeline_detail = "Held request remains unsent".into();
            });
            self.speak_local("I still need the worker number. I have not sent the request.");
            return true;
        };
        let Some(target) = self.worker_target_for_label(label) else {
            let question = worker_clarification_question(&self.active_worker_choices());
            self.add_system_message(&format!("Worker {label} is not active. {question}"));
            self.update_view(|view| {
                view.status = "Choose an active worker".into();
                view.pipeline_detail = "Held request remains unsent".into();
            });
            self.speak_local(&format!(
                "Worker {label} is not active. Please choose an active worker."
            ));
            return true;
        };

        self.pending_worker_clarification = None;
        let routed_prompt = if is_worker_selection_only(response) {
            pending.prompt
        } else {
            format!(
                "{}\n\nWorker-selection clarification: {}",
                pending.prompt,
                response.trim()
            )
        };
        self.route_addition_to_worker(target, routed_prompt, pending.engine);
        true
    }

    fn route_addition_to_worker(
        &mut self,
        target: WorkerTarget,
        prompt: String,
        fallback_engine: WorkerEngine,
    ) {
        match target {
            WorkerTarget::Primary => {
                let active_engine = self.active_engine.unwrap_or(fallback_engine);
                let steered = self
                    .agent_control
                    .as_ref()
                    .is_some_and(|control| control.steer(prompt.clone()).is_ok());
                if steered {
                    self.unconfirmed_additions.push_back(prompt);
                    self.persist_session();
                    self.update_view(|view| {
                        view.status = "Adding to worker 1".into();
                        view.pipeline_stage = PipelineStage::Transferring;
                        view.pipeline_detail = "Routing the addition to the primary worker".into();
                    });
                    self.speak_local("Sending that addition to worker one.");
                } else if self.agent_control.is_none() && self.active_request.is_some() {
                    if let Some(request) = self.active_request.as_mut() {
                        request.additions.push(prompt);
                        let queued = request.additions.len();
                        self.update_view(|view| {
                            view.queued_additions = queued;
                            view.status = "Saved for worker 1 reconnect".into();
                            view.pipeline_stage = PipelineStage::Reconnecting;
                            view.pipeline_detail =
                                format!("Worker 1 request and {queued} addition(s) saved locally");
                        });
                    }
                    self.persist_session();
                    self.speak_local("Worker one is reconnecting. I saved that addition for it.");
                } else {
                    self.pending_prompts.push_back(PendingPrompt {
                        text: prompt,
                        engine: active_engine,
                    });
                    self.update_view(|view| {
                        view.status = "Queued for worker 1".into();
                        view.pipeline_stage = PipelineStage::Transferring;
                        view.pipeline_detail =
                            "Worker 1 keeps working • addition runs next in its primary session"
                                .into();
                    });
                    self.speak_local("That addition is queued for worker one.");
                }
            }
            WorkerTarget::Parallel(turn_id) => {
                let Some(label) = self.parallel_turns.get(&turn_id).map(|turn| turn.label) else {
                    self.ask_which_worker(prompt, fallback_engine);
                    return;
                };
                let steered = self
                    .parallel_turns
                    .get(&turn_id)
                    .and_then(|turn| turn.control.as_ref())
                    .is_some_and(|control| control.steer(prompt.clone()).is_ok());
                if steered {
                    if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                        turn.unconfirmed_additions.push_back(prompt);
                    }
                    self.update_view(|view| {
                        view.status = format!("Adding to worker {label}");
                        view.pipeline_stage = PipelineStage::Transferring;
                        view.pipeline_detail =
                            format!("Routing the addition to parallel worker {label}");
                    });
                    self.speak_local(&format!("Sending that addition to worker {label}."));
                } else {
                    if let Some(turn) = self.parallel_turns.get_mut(&turn_id) {
                        if turn.control.is_none() {
                            turn.request.additions.push(prompt);
                        } else {
                            turn.queued_follow_ups.push(prompt);
                        }
                    }
                    self.update_view(|view| {
                        view.status = format!("Saved for worker {label}");
                        view.pipeline_stage = PipelineStage::Transferring;
                        view.pipeline_detail = format!(
                            "Worker {label} could not accept live steering • addition retained for its session"
                        );
                    });
                    self.speak_local(&format!(
                        "Worker {label} could not accept it live. I saved the addition for that worker."
                    ));
                }
            }
        }
    }

    fn any_worker_reconnecting(&self) -> bool {
        self.reconnect_attempt > 0
            || self
                .parallel_turns
                .values()
                .any(|turn| turn.reconnect_attempt > 0)
    }

    fn refresh_worker_view(&self) {
        let count = self.total_active_workers();
        let reconnecting = self.any_worker_reconnecting();
        self.update_view(|view| {
            view.active_workers = count;
            view.working = count > 0;
            view.connectivity = if reconnecting {
                ConnectivityState::Reconnecting
            } else {
                ConnectivityState::Online
            };
            if reconnecting {
                view.status = "One or more workers are reconnecting".into();
                view.pipeline_stage = PipelineStage::Reconnecting;
                view.pipeline_detail = format!("{count} worker session(s) retained locally");
            } else if count > 0 {
                view.status = if count == 1 {
                    "One worker is still working".into()
                } else {
                    format!("{count} workers are working in parallel")
                };
                view.pipeline_stage = PipelineStage::Processing;
                view.pipeline_detail = format!("{count} active worker session(s)");
            } else if view.listening {
                view.status = "Listening for “Hey Codex” or “Hey Claude”".into();
                view.pipeline_stage = PipelineStage::Listening;
                view.pipeline_detail =
                    "Microphone live • waiting for speech • Whisper ready".into();
            } else {
                view.status = "Ready".into();
                view.pipeline_stage = PipelineStage::Paused;
                view.pipeline_detail = "Voice pipeline is idle".into();
            }
        });
    }

    fn restore_pending_request(&mut self) {
        let Some(request) = self.active_request.as_ref() else {
            return;
        };
        let prompt = request.prompt.clone();
        let queued = request.additions.len();
        self.add_message(ChatRole::User, prompt);
        self.add_system_message(
            "Restoring an interrupted request from local storage and reconnecting automatically.",
        );
        let engine = self.current_engine();
        self.active_engine = Some(engine);
        self.session_engine.get_or_insert(engine);
        self.next_turn_id = self.next_turn_id.wrapping_add(1);
        let turn_id = self.next_turn_id;
        self.active_turn = Some(turn_id);
        self.reconnect_attempt = 1;
        self.turn_failure = None;
        self.turn_had_message = false;
        self.voice_started_at = Some(Instant::now());
        let delay = reconnect_delay(self.reconnect_attempt);
        let worker_count = self.total_active_workers();
        self.update_view(|view| {
            view.working = true;
            view.active_workers = worker_count;
            view.connectivity = ConnectivityState::Reconnecting;
            view.reconnect_attempt = self.reconnect_attempt;
            view.queued_additions = queued;
            view.status = "Restoring saved request".into();
            view.pipeline_stage = PipelineStage::Reconnecting;
            view.pipeline_detail = format!(
                "Request recovered from disk • reconnecting in {}s",
                delay.as_secs()
            );
        });
        self.schedule_reconnect(turn_id, self.reconnect_attempt, delay);
    }

    fn start_reconnect(&mut self, turn_id: u64) {
        let Some(request) = self.active_request.clone() else {
            return;
        };
        let engine = self.current_engine();
        let config = self.settings.remote_config_for_engine(engine);
        if let Err(error) = config.validate() {
            self.active_turn = None;
            self.active_engine = None;
            self.active_request = None;
            self.reconnect_attempt = 0;
            let remaining_workers = self.parallel_turns.len();
            self.update_view(|view| {
                view.working = remaining_workers > 0;
                view.active_workers = remaining_workers;
                view.connectivity = ConnectivityState::Online;
                view.reconnect_attempt = 0;
                view.queued_additions = 0;
                view.status = "Reconnect needs setup".into();
                view.pipeline_stage = PipelineStage::Error;
                view.pipeline_detail = error.to_string();
            });
            self.persist_session();
            self.add_system_message(&format!("Could not reconnect: {error}"));
            self.speak_local("Reconnect needs an updated worker configuration.");
            return;
        }

        let recovery = reconnect_prompt(&request, self.session_id.is_some(), config.engine);
        let voice_prompt = execution_worker_prompt(
            &recovery,
            self.settings.local_worker,
            config.engine,
            config.sandbox,
            self.settings.deep_relay,
            &self.settings.ssh_target,
            &self.settings.workspace,
        );
        let reporter = self.settings.deep_relay.then(|| ReporterConfig {
            executable: self.settings.local_codex_executable.trim().to_owned(),
            primary_model: self.settings.reporter_model.trim().to_owned(),
            fallback_model: self.settings.reporter_fallback_model().into(),
            worker_host: config.host,
            worker_target: config.ssh_target.clone(),
            worker_workspace: config.workspace.clone(),
        });

        self.turn_failure = None;
        self.transfer_started_at = Some(Instant::now());
        self.codex_started_at = None;
        self.acknowledged_turn = Some(turn_id);
        self.update_view(|view| {
            view.working = true;
            view.connectivity = ConnectivityState::Reconnecting;
            view.status = "Checking internet connection".into();
            view.pipeline_stage = PipelineStage::Reconnecting;
            view.pipeline_detail = format!(
                "Reconnect attempt {} • resuming saved worker session",
                self.reconnect_attempt
            );
        });
        self.agent_control = Some(spawn_turn(
            turn_id,
            config,
            reporter,
            self.session_id.clone(),
            voice_prompt,
            self.agent_tx.clone(),
        ));
    }

    fn schedule_reconnect(&self, turn_id: u64, attempt: u32, delay: Duration) {
        let sender = self.timed_tx.clone();
        tauri::async_runtime::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = sender.send(TimedEvent::Reconnect { turn_id, attempt });
        });
    }

    fn schedule_parallel_reconnect(&self, turn_id: u64, attempt: u32, delay: Duration) {
        let sender = self.timed_tx.clone();
        tauri::async_runtime::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = sender.send(TimedEvent::ParallelReconnect { turn_id, attempt });
        });
    }

    fn confirm_reconnected(&mut self) {
        if self.reconnect_attempt == 0 {
            return;
        }
        self.reconnect_attempt = 0;
        self.persist_session();
        let parallel_reconnecting = self.any_worker_reconnecting();
        self.update_view(|view| {
            view.connectivity = if parallel_reconnecting {
                ConnectivityState::Reconnecting
            } else {
                ConnectivityState::Online
            };
            view.reconnect_attempt = 0;
            view.queued_additions = 0;
            view.status = if parallel_reconnecting {
                "Primary worker restored • another worker is reconnecting".into()
            } else {
                "Connection stable — worker responding".into()
            };
        });
        self.speak_local(if self.settings.local_worker {
            "Back online. The Mac worker is responding."
        } else {
            "Back online. The SSH worker is responding."
        });
    }

    fn cancel_active_turn(&mut self, speak_confirmation: bool) {
        self.pending_worker_clarification = None;
        let had_primary = self.active_turn.take().is_some();
        let parallel_count = self.cancel_parallel_turns();
        if had_primary || parallel_count > 0 {
            self.active_engine = None;
            if let Some(control) = self.agent_control.take() {
                control.cancel();
            }
            self.active_request = None;
            self.cancellation = CancellationState::Idle;
            self.voice_suppressed_turn = None;
            self.parallel_voice = ParallelVoiceState::Enabled;
            self.unconfirmed_additions.clear();
            self.reconnect_attempt = 0;
            self.turn_failure = None;
            self.turn_had_message = false;
            self.pending_prompts.clear();
            self.update_view(|view| {
                view.working = false;
                view.active_workers = 0;
                view.connectivity = ConnectivityState::Online;
                view.reconnect_attempt = 0;
                view.queued_additions = 0;
                view.status = if view.speaking {
                    "Speaking".into()
                } else if view.listening {
                    "Listening for “Hey Codex” or “Hey Claude”".into()
                } else {
                    "Ready".into()
                };
                view.pipeline_stage = if view.listening {
                    PipelineStage::Listening
                } else {
                    PipelineStage::Paused
                };
                view.pipeline_detail = "Worker turn cancelled".into();
            });
            self.persist_session();
            let cancelled_count = usize::from(had_primary) + parallel_count;
            self.add_system_message(&format!("Cancelled {cancelled_count} active worker(s)."));
            if speak_confirmation {
                self.speak_force("Cancelling all active workers.");
            }
        } else if speak_confirmation {
            self.speak_force("There are no active workers.");
        }
    }

    fn cancel_parallel_turns(&mut self) -> usize {
        let count = self.parallel_turns.len();
        for (_, mut turn) in self.parallel_turns.drain() {
            if let Some(control) = turn.control.take() {
                control.cancel();
            }
        }
        self.parallel_voice = ParallelVoiceState::Enabled;
        count
    }

    fn request_cancel(&mut self, speak_confirmation: bool) {
        let discarded_clarification = self.pending_worker_clarification.take().is_some();
        let parallel_count = self.cancel_parallel_turns();
        let Some(turn_id) = self.active_turn else {
            if parallel_count > 0 {
                self.pending_prompts.clear();
                self.refresh_worker_view();
                self.add_system_message(&format!("Cancelled {parallel_count} parallel worker(s)."));
                if speak_confirmation {
                    self.speak_force("Cancelled all parallel workers.");
                }
            } else if discarded_clarification {
                self.add_system_message("Discarded the held worker-routing request.");
                if speak_confirmation {
                    self.speak_force("Discarded the held request.");
                }
            } else if speak_confirmation {
                self.speak_force("There are no active workers.");
            }
            return;
        };
        if self.cancellation == CancellationState::Requested {
            return;
        }

        self.interruption = InterruptionState::None;
        self.speaking = false;
        self.voice_suppressed_turn = Some(turn_id);
        self.speech.send(SpeechCommand::StopSpeaking);
        self.active_request = None;
        self.unconfirmed_additions.clear();
        self.reconnect_attempt = 0;
        self.turn_failure = None;
        self.pending_prompts.clear();
        self.persist_session();

        if let Some(control) = self.agent_control.take() {
            control.cancel();
            self.cancellation = CancellationState::Requested;
            self.update_view(|view| {
                view.speaking = false;
                view.working = true;
                view.active_workers = 1;
                view.connectivity = ConnectivityState::Online;
                view.reconnect_attempt = 0;
                view.queued_additions = 0;
                view.status = "Cancelling worker".into();
                view.pipeline_stage = PipelineStage::Processing;
                view.pipeline_detail = "Cancellation sent • waiting for worker confirmation".into();
            });
            self.add_system_message(if parallel_count > 0 {
                "Cancelled the parallel workers and requested cancellation of the primary worker."
            } else {
                "Cancellation requested; waiting for the worker to stop."
            });
            if speak_confirmation {
                self.speak_force(if parallel_count > 0 {
                    "Cancelling all active workers."
                } else {
                    "Cancelling the worker."
                });
            }
        } else {
            self.cancel_active_turn(false);
            if speak_confirmation {
                self.speak_force("Worker cancelled.");
            }
        }
    }

    fn stop_voice(&mut self) {
        self.interruption = InterruptionState::None;
        self.speaking = false;
        self.voice_suppressed_turn = self.active_turn;
        self.parallel_voice = if self.parallel_turns.is_empty() {
            ParallelVoiceState::Enabled
        } else {
            ParallelVoiceState::Suppressed
        };
        self.speech.send(SpeechCommand::StopSpeaking);
        self.update_view(|view| {
            view.speaking = false;
            view.status = if view.working {
                "Voice stopped — worker is still working".into()
            } else if view.listening {
                "Listening for “Hey Codex” or “Hey Claude”".into()
            } else {
                "Voice stopped".into()
            };
            view.pipeline_detail = if view.working {
                "Neural voice generation/playback stopped for this task".into()
            } else {
                "Neural voice generation/playback stopped".into()
            };
        });
    }

    fn new_session(&mut self) {
        self.new_session_for(None);
    }

    fn new_session_for(&mut self, requested_engine: Option<WorkerEngine>) {
        let engine = requested_engine.unwrap_or_else(|| self.default_engine());
        self.cancel_active_turn(false);
        self.clear_session();
        self.session_engine = Some(engine);
        self.persist_session();
        let worker = worker_name(engine);
        self.add_system_message(&format!("Started a fresh {worker} session."));
        self.speak_local(&format!("Started a fresh {worker} session."));
    }

    fn clear_session(&mut self) {
        self.pending_worker_clarification = None;
        self.session_id = None;
        self.session_engine = None;
        if self.active_turn.is_none() {
            self.active_engine = None;
        }
        self.update_view(|view| {
            view.session_id = None;
            view.last_usage = None;
            view.session_usage = roadie_core::TokenUsage::default();
            view.last_reporter_usage = None;
            view.session_reporter_usage = roadie_core::TokenUsage::default();
        });
        self.persist_session();
    }

    fn set_mac_workdir(&mut self, requested_workspace: &str) {
        let workspace = match resolve_mac_workspace(requested_workspace) {
            Ok(workspace) => workspace,
            Err(error) => {
                self.add_system_message(&error);
                self.speak_force(&error);
                return;
            }
        };
        let mut next = self.settings.clone();
        next.local_workspace.clone_from(&workspace);
        if let Err(error) = next.validate() {
            let message = format!("I could not use that Mac working directory. {error}");
            self.add_system_message(&message);
            self.speak_force(&message);
            return;
        }
        if let Err(error) = save_settings(&self.settings_path, &next) {
            let message = format!("I could not persist the Mac working directory. {error}");
            self.add_system_message(&message);
            self.speak_force(&message);
            return;
        }

        self.cancel_active_turn(false);
        self.clear_session();
        self.settings = next;
        write_shared(&self.shared)
            .settings
            .clone_from(&self.settings);
        let _ = self.app.emit("roadie://settings", self.settings.clone());
        warm_connection(self.settings.remote_config());
        self.persist_session();
        self.update_view(|view| {
            view.status = "Mac working directory updated".into();
            view.pipeline_detail = format!("Mac workdir: {workspace} • fresh session ready");
        });
        self.add_system_message(&format!(
            "Mac workdir set to {workspace}. Started a fresh session."
        ));
        self.speak_force("Mac working directory updated. I started a fresh session.");
    }

    fn repeat_last(&self) {
        if let Some(message) = self.last_agent_message.as_ref() {
            self.speak_local(message);
        } else {
            self.speak_local("There is no worker response to repeat yet.");
        }
    }

    fn speak_status_for(&self, requested_engine: Option<WorkerEngine>) {
        let worker_count = self.total_active_workers();
        let parallel_status = format!(
            "There are {worker_count} independent workers running in parallel. Give a normal addition to update the primary worker, or say in parallel or start another worker to create another worker."
        );
        let selected_engine = if self.active_turn.is_some() {
            self.current_engine()
        } else {
            requested_engine.unwrap_or_else(|| self.current_engine())
        };
        let status = if worker_count > 1 {
            parallel_status.as_str()
        } else if self.reconnect_attempt > 0 {
            "The worker link is reconnecting. Roadie saved your active request and additions locally and will resume automatically."
        } else if self.active_turn.is_none() && !self.parallel_turns.is_empty() {
            "One independent worker is still working."
        } else if self.active_turn.is_some() {
            if self.settings.local_worker {
                if selected_engine == WorkerEngine::Claude {
                    "The Mac Claude Code worker is working."
                } else {
                    "The Mac Codex worker is working."
                }
            } else {
                "The remote Codex worker is working through SSH."
            }
        } else if self.session_id.is_some() {
            if self.settings.local_worker {
                if selected_engine == WorkerEngine::Claude {
                    "The Mac Claude Opus 5 worker is ready and its session is connected."
                } else {
                    "The Mac Codex worker is ready and its session is connected."
                }
            } else {
                "The remote Codex worker is ready and its SSH session is connected."
            }
        } else if !self.settings.local_worker && self.settings.ssh_target.trim().is_empty() {
            "Codex Roadie is listening, but SSH setup is not complete."
        } else if selected_engine == WorkerEngine::Claude {
            "The Mac Claude Code worker is ready for a new task."
        } else if self.settings.local_worker {
            "The Mac Codex worker is ready for a new task."
        } else {
            "The remote Codex worker is ready for a new task."
        };
        self.speak_local(status);
    }

    fn repeat_listening(&self) {
        self.speech.send(SpeechCommand::StartListening {
            locale: self.settings.locale.clone(),
            silence_ms: self.settings.silence_ms,
            wake_phrase: self.settings.wake_phrase.clone(),
            voice_isolation: self.settings.voice_isolation,
        });
    }

    fn start_listening(&mut self) {
        if self.listening_requested {
            return;
        }
        self.listening_requested = true;
        self.repeat_listening();
        self.update_view(|view| {
            view.status = "Starting microphone".into();
            view.pipeline_stage = PipelineStage::Loading;
            view.pipeline_detail = "Opening persistent CoreAudio capture".into();
        });
    }

    fn restart_listening(&mut self) {
        self.listening_requested = true;
        self.repeat_listening();
        if self.reconnect_attempt > 0 {
            return;
        }
        self.update_view(|view| {
            view.status = "Starting microphone".into();
            view.pipeline_stage = PipelineStage::Loading;
            view.pipeline_detail = "Resetting local capture for the next utterance".into();
        });
    }

    fn speak_local(&self, text: &str) {
        if self.parallel_voice == ParallelVoiceState::Suppressed
            || (self.active_turn.is_some() && self.voice_suppressed_turn == self.active_turn)
        {
            return;
        }
        self.speak_force(text);
    }

    fn speak_force(&self, text: &str) {
        self.speech.send(SpeechCommand::Speak {
            text: text.to_owned(),
            rate: self.settings.speech_rate,
        });
    }

    fn add_system_message(&mut self, text: &str) {
        self.add_message(ChatRole::System, text.to_owned());
    }

    fn add_message(&mut self, role: ChatRole, text: String) {
        self.next_message_id = self.next_message_id.wrapping_add(1);
        let message = ChatMessage {
            id: self.next_message_id,
            role,
            text,
        };
        self.update_view(|view| view.push_message(message));
    }

    fn update_view(&self, update: impl FnOnce(&mut ViewState)) {
        let snapshot = {
            let mut shared = write_shared(&self.shared);
            update(&mut shared.view);
            shared.view.clone()
        };
        let _ = self.app.emit("roadie://state", snapshot);
    }

    fn persist_session(&self) {
        let view = crate::models::read_shared(&self.shared).view.clone();
        let engine = self.current_engine();
        let config = self.settings.remote_config_for_engine(engine);
        if let Err(error) = save_runtime(
            &self.runtime_path,
            &RuntimeState {
                session_id: self.session_id.clone(),
                session_host: Some(config.host),
                session_engine: Some(engine),
                session_usage: view.session_usage,
                reporter_usage: view.session_reporter_usage,
                pending_request: self.active_request.clone(),
            },
        ) {
            self.emit_diagnostic(format!("Could not persist session: {error}"));
        }
    }

    fn emit_diagnostic(&self, message: String) {
        let _ = self.app.emit("roadie://diagnostic", message);
    }
}

fn reconnect_prompt(request: &PendingRequest, has_thread: bool, engine: WorkerEngine) -> String {
    let mut prompt = if has_thread {
        let session = match engine {
            WorkerEngine::Codex => "persisted Codex thread",
            WorkerEngine::Claude => "persisted Claude Code session",
        };
        format!(
            "The internet connection interrupted the previous attempt. Resume this same request safely from the {session}. Inspect the current state first and do not repeat completed commands, file edits, deployments, messages, purchases, or other side effects. Continue from the last verified state.\n\nORIGINAL REQUEST:\n{}",
            request.prompt
        )
    } else {
        request.prompt.clone()
    };
    if !request.additions.is_empty() {
        prompt.push_str("\n\nADDITIONS RECEIVED WHILE OFFLINE:");
        for addition in &request.additions {
            prompt.push_str("\n- ");
            prompt.push_str(addition);
        }
        prompt.push_str("\n\nTreat these additions as part of the same request.");
    }
    prompt
}

fn parallel_follow_up_prompt(request: &PendingRequest, has_thread: bool) -> String {
    let mut prompt = if has_thread {
        "Continue the same task in this existing worker session. Inspect the current state first and do not repeat work that is already complete. Apply these held user additions:"
            .to_owned()
    } else {
        format!(
            "Continue this worker's task safely. Inspect the current state first and preserve completed work.\n\nORIGINAL REQUEST:\n{}\n\nHELD USER ADDITIONS:",
            request.prompt
        )
    };
    for addition in &request.additions {
        prompt.push_str("\n- ");
        prompt.push_str(addition);
    }
    prompt
}

fn reconnect_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(5);
    let base_ms = 1_000_u64 << exponent;
    let jitter_ms = u64::from(attempt.wrapping_mul(211) % 700);
    Duration::from_millis(
        base_ms
            .saturating_add(jitter_ms)
            .min(MAX_RECONNECT_DELAY.as_secs().saturating_mul(1_000)),
    )
}

const fn worker_name(engine: WorkerEngine) -> &'static str {
    match engine {
        WorkerEngine::Codex => "Codex",
        WorkerEngine::Claude => "Claude",
    }
}

fn targets_different_worker(
    requested_engine: Option<WorkerEngine>,
    active_engine: WorkerEngine,
) -> bool {
    requested_engine.is_some_and(|requested| requested != active_engine)
}

const fn agent_event_turn_id(event: &AgentEvent) -> u64 {
    match event {
        AgentEvent::ThreadStarted { turn_id, .. }
        | AgentEvent::Transfer { turn_id, .. }
        | AgentEvent::Started { turn_id }
        | AgentEvent::Steered { turn_id }
        | AgentEvent::SteerFailed { turn_id, .. }
        | AgentEvent::ReporterStatus { turn_id, .. }
        | AgentEvent::Usage { turn_id, .. }
        | AgentEvent::TurnCommitted { turn_id }
        | AgentEvent::WorkerMessage { turn_id, .. }
        | AgentEvent::VoiceMessage { turn_id, .. }
        | AgentEvent::Failed { turn_id, .. }
        | AgentEvent::Diagnostic { turn_id, .. }
        | AgentEvent::Exited { turn_id, .. } => *turn_id,
    }
}

fn strip_parallel_request_prefix(prompt: &str) -> (&str, bool) {
    let prompt = prompt.trim();
    let lowercased = prompt.to_ascii_lowercase();
    for prefix in [
        "in parallel ",
        "in parallel, ",
        "start another worker to ",
        "can you start another worker to ",
        "please start another worker to ",
        "start a new worker to ",
        "start a parallel worker to ",
        "have another worker to ",
        "have another worker ",
        "new worker ",
        "parallel task ",
        "separate task ",
        "separate task: ",
        "new task ",
        "new task: ",
        "fire up a subagent to ",
        "spawn a subagent to ",
    ] {
        if lowercased.starts_with(prefix) {
            return (prompt[prefix.len()..].trim(), true);
        }
    }
    (prompt, false)
}

fn referenced_worker(prompt: &str) -> WorkerReference {
    let normalized = prompt
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>();
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let mut labels = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        let candidate = if *token == "worker" {
            tokens
                .get(index + 1)
                .and_then(|next| worker_label_token(next))
                .or_else(|| {
                    (tokens.get(index + 1) == Some(&"number"))
                        .then(|| {
                            tokens
                                .get(index + 2)
                                .and_then(|next| worker_label_token(next))
                        })
                        .flatten()
                })
        } else if tokens.get(index + 1) == Some(&"worker") {
            worker_label_token(token)
        } else {
            None
        };
        if let Some(label) = candidate
            && !labels.contains(&label)
        {
            labels.push(label);
        }
    }

    match labels.as_slice() {
        [] => WorkerReference::None,
        [label] => WorkerReference::Label(*label),
        _ => WorkerReference::Ambiguous,
    }
}

fn worker_label_token(token: &str) -> Option<u64> {
    match token {
        "one" | "first" | "primary" | "main" => Some(1),
        "two" | "too" | "second" => Some(2),
        "three" | "third" => Some(3),
        _ => token.parse::<u64>().ok(),
    }
}

fn clarification_worker_reference(response: &str) -> WorkerReference {
    let explicit = referenced_worker(response);
    if explicit != WorkerReference::None {
        return explicit;
    }
    let normalized = response
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>();
    let mut labels = normalized
        .split_whitespace()
        .filter_map(worker_label_token)
        .collect::<Vec<_>>();
    labels.sort_unstable();
    labels.dedup();
    match labels.as_slice() {
        [label] if is_worker_selection_only(response) => WorkerReference::Label(*label),
        [] => WorkerReference::None,
        _ => WorkerReference::Ambiguous,
    }
}

fn is_worker_selection_only(prompt: &str) -> bool {
    let normalized = prompt
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>();
    normalized.split_whitespace().all(|token| {
        matches!(
            token,
            "worker"
                | "number"
                | "one"
                | "two"
                | "too"
                | "three"
                | "first"
                | "second"
                | "third"
                | "primary"
                | "main"
                | "the"
                | "please"
                | "1"
                | "2"
                | "3"
        )
    })
}

fn compact_task_brief(prompt: &str) -> String {
    const MAX_CHARS: usize = 72;
    let compact = prompt.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_CHARS {
        return compact;
    }
    let mut brief = compact.chars().take(MAX_CHARS - 1).collect::<String>();
    brief.push('…');
    brief
}

fn worker_clarification_question(choices: &[(u64, String)]) -> String {
    let options = choices
        .iter()
        .map(|(label, prompt)| format!("worker {label} — {}", compact_task_brief(prompt)))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "Which worker should receive that request: {options}? Say the worker number. I have not forwarded it yet."
    )
}

fn is_follow_up_request(prompt: &str) -> bool {
    let prompt = prompt.trim().trim_start_matches('/').to_ascii_lowercase();
    [
        "also ",
        "and ",
        "and also ",
        "by the way ",
        "btw ",
        "add ",
        "additionally ",
        "make sure ",
        "one more thing ",
        "can you also ",
        "could you also ",
        "that ",
        "that's ",
        "thats ",
        "this ",
        "it ",
        "continue",
        "resume",
        "keep ",
        "go ahead",
        "yes",
        "okay",
        "ok ",
        "now ",
        "next ",
    ]
    .iter()
    .any(|prefix| prompt.starts_with(prefix))
}

fn should_launch_parallel(
    force_parallel: bool,
    requested_engine: Option<WorkerEngine>,
    active_engine: WorkerEngine,
) -> bool {
    force_parallel || targets_different_worker(requested_engine, active_engine)
}

fn should_start_fresh_session(prompt: &str) -> bool {
    if is_follow_up_request(prompt) {
        return false;
    }
    let prompt = prompt.trim().trim_start_matches('/').to_ascii_lowercase();
    [
        "create a new project",
        "build a new project",
        "start a new project",
        "make a new project",
        "create a new app",
        "build a new app",
        "start a new app",
        "make a new app",
        "create a new application",
        "build a new application",
        "start a new application",
        "make a new application",
        "i want to create a new project",
        "i want to build a new project",
        "i want to start a new project",
        "i want to create a new app",
        "i want to build a new app",
        "i want to create a new application",
    ]
    .iter()
    .any(|prefix| prompt.starts_with(prefix))
}

fn parallel_worker_prompt(label: u64, execution_prompt: &str) -> String {
    format!(
        "You are Roadie parallel worker {label}. You have an independent worker session and must execute this explicitly parallel task concurrently instead of trying to steer or wait for another Roadie worker. The process working directory is only launch context; verify the user's intended project root before the first edit and never treat an unrelated app as the task target merely because it is the current directory. Other workers may be changing the same workspace, so inspect current files before each edit, preserve unrelated concurrent changes, and never undo work you did not author. Identify meaningful progress and the final result as worker {label}.\n\n{execution_prompt}"
    )
}

fn is_transient_connectivity_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "network error",
        "network is unreachable",
        "internet connection",
        "internet connectivity probe failed",
        "offline",
        "connection reset",
        "connection refused",
        "connection closed",
        "connection aborted",
        "app-server connection closed unexpectedly",
        "app-server became unresponsive",
        "broken pipe",
        "timed out",
        "timeout",
        "temporary failure in name resolution",
        "could not resolve host",
        "failed to lookup address",
        "dns error",
        "error sending request",
        "failed to send request",
        "stream disconnected",
        "stream unresponsive",
        "route failed sustained probes",
        "upstream connect error",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "server overloaded; retry later",
        "http status 502",
        "http status 503",
        "http status 504",
        "reconnecting",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn concise_error(stderr: &str) -> String {
    stderr
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map_or("No error detail was returned.", str::trim)
        .chars()
        .take(280)
        .collect()
}

fn is_ssh_transport_failure(stderr: &str) -> bool {
    stderr.lines().any(|line| {
        let line = line.trim().to_ascii_lowercase();
        line.starts_with("ssh: ")
            || line.starts_with("could not launch ssh:")
            || line.starts_with("kex_exchange_identification:")
            || line.starts_with("ssh_exchange_identification:")
            || line.starts_with("host key verification failed")
            || line.contains("permission denied (publickey")
    })
}

fn execution_worker_prompt(
    prompt: &str,
    local_worker: bool,
    engine: WorkerEngine,
    sandbox: SandboxMode,
    has_reporter: bool,
    ssh_target: &str,
    remote_workspace: &str,
) -> String {
    let location = if local_worker {
        let identity = match engine {
            WorkerEngine::Codex => "You are the Mac-local Codex execution worker",
            WorkerEngine::Claude => "You are the Mac-local Claude Code execution worker",
        };
        let access = match sandbox {
            SandboxMode::ReadOnly => {
                "Your active execution profile is read-only. Report that boundary precisely if a requested write cannot proceed"
            }
            SandboxMode::WorkspaceWrite => {
                "Your active execution profile is workspace-write. Work within the selected workspace and report any operating-system boundary precisely"
            }
            SandboxMode::DangerFullAccess => {
                "The user explicitly enabled Full Mac access, so you are not running in a filesystem sandbox. macOS privacy consent and unavailable credentials remain separate boundaries"
            }
        };
        let device_guidance = if sandbox == SandboxMode::DangerFullAccess {
            " For Android visual/device QA, use a Mac-local ADB installation when available; for iOS use `/usr/bin/xcrun simctl` and other Mac-local Xcode tools."
        } else {
            " Device sockets may require the user to opt into Full Mac access; do not claim device tooling is available until you verify it."
        };
        format!(
            "{identity}. {access}.{device_guidance} Never route Mac-only visual QA through the remote SSH host merely because the source repository is there. If configured, a source workspace is available over key-only SSH as `{ssh_target}` at `{remote_workspace}` for remote-side code, builds, or tests. Distinguish a disconnected device from an unavailable device bridge and use deterministic tests while no device is connected."
        )
    } else {
        format!(
            "You are the remote Codex execution worker. Roadie already launched this exact process on `{ssh_target}` through key-only SSH and set its working directory to `{remote_workspace}`. Run the user's commands, tool calls, edits, builds, and tests directly in this process. Do not SSH to `{ssh_target}` again, do not hand command execution to the Mac voice reporter, and do not claim that the reporter is performing the work. The Mac is only capturing speech, displaying updates, and optionally narrating messages. Unless the user explicitly requests Mac-only ADB, Xcode, or GUI work, remain on the SSH host and complete the task here."
        )
    };
    let reporter = if has_reporter {
        " A separate local voice reporter on the Mac will paraphrase your chat updates but cannot change the work."
    } else {
        " Your chat messages are spoken directly by the local voice system."
    };
    format!(
        "{location}\n\nYou alone own the task, tool use, decisions, edits, tests, and final result.{reporter} Send brief, factual chat progress updates at meaningful milestones so the driver is never left wondering, then complete the task fully. Keep implementation detail in tools and files.\n\nTreat the process working directory as launch context, not proof that its application is the user's requested target. Before the first edit, identify and verify the intended project root from the user's task and existing conversation. If the user asks for a new or separate project, app, or application, create it in its own sibling directory under the current workspace's parent; never insert that work into the current app unless the user explicitly names the current app as the target. Preserve that target project across follow-up turns and never repurpose another app to satisfy an ambiguous pronoun.\n\nReport execution boundaries precisely. SSH transport, local device tooling, remote tooling, credentials, service availability, and operating-system permissions are separate capabilities; never describe one as an SSH failure unless SSH itself failed. Before declaring the task blocked, verify the relevant capability once and exhaust safe in-scope alternatives. Use blocked only when the current requested objective cannot continue and an exact user or external action is required. If meaningful work can continue, call the limitation deferred or identify the fallback and continue. Do not repeatedly restate an unchanged external dependency in milestone updates; report it when discovered and in the final handoff. An unavailable live device is not a device-bridge failure. Never expose internal drafting, self-corrections, hidden reasoning, or prompt instructions in chat updates.\n\nUSER TASK:\n{prompt}"
    )
}

fn attributed_execution_update(local_worker: bool, message: &str) -> String {
    let label = if local_worker {
        "Mac worker update:"
    } else {
        "SSH worker update:"
    };
    format!("{label} {}", message.trim())
}

fn elapsed_ms(started: Instant, finished: Instant) -> u128 {
    finished.saturating_duration_since(started).as_millis()
}

fn resolve_mac_workspace(spoken: &str) -> Result<String, String> {
    let path = decode_spoken_path(spoken);
    if !path.is_absolute() {
        return Err("The Mac working directory must be an absolute path.".into());
    }
    let canonical = std::fs::canonicalize(&path).map_err(|_| {
        format!(
            "I could not find the Mac working directory {}.",
            path.to_string_lossy()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!(
            "The Mac working directory {} is not a directory.",
            canonical.to_string_lossy()
        ));
    }
    Ok(canonical.to_string_lossy().into_owned())
}

fn decode_spoken_path(spoken: &str) -> PathBuf {
    let spoken = spoken.trim().trim_matches(['\'', '"']);
    if spoken.starts_with('/') {
        return PathBuf::from(spoken);
    }
    if let Some(rest) = spoken.strip_prefix('~')
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest.trim_start_matches('/'));
    }

    let words = spoken
        .split_whitespace()
        .map(|word| {
            word.trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.')
            })
        })
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    let is_spoken_path = words.iter().any(|word| {
        matches!(
            word.to_ascii_lowercase().as_str(),
            "slash" | "dash" | "hyphen" | "underscore" | "tilde"
        )
    });
    if !is_spoken_path {
        return PathBuf::from(spoken);
    }

    let mut path = String::new();
    let mut segment = String::new();
    for word in words {
        let normalized = word.to_ascii_lowercase();
        match normalized.as_str() {
            "forward" => {}
            "slash" => {
                push_spoken_segment(&mut path, &mut segment);
                if !path.ends_with('/') {
                    path.push('/');
                }
            }
            "dash" | "hyphen" => segment.push('-'),
            "underscore" => segment.push('_'),
            "dot" | "period" => segment.push('.'),
            "home" | "tilde" if path.is_empty() && segment.is_empty() => {
                if let Some(home) = std::env::var_os("HOME") {
                    path.push_str(&home.to_string_lossy());
                }
            }
            _ => {
                if !segment.is_empty()
                    && !segment.ends_with(['-', '_', '.'])
                    && !word.starts_with(['-', '_', '.'])
                {
                    segment.push('-');
                }
                segment.push_str(word);
            }
        }
    }
    push_spoken_segment(&mut path, &mut segment);
    PathBuf::from(path)
}

fn push_spoken_segment(path: &mut String, segment: &mut String) {
    if segment.is_empty() {
        return;
    }
    if !path.is_empty() && !path.ends_with('/') {
        path.push('/');
    }
    path.push_str(segment);
    segment.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detected_speech_pauses_any_active_playback_immediately() {
        assert!(should_pause_for_detected_speech(true, false, false));
        assert!(should_pause_for_detected_speech(false, true, false));
        assert!(!should_pause_for_detected_speech(false, false, false));
        assert!(!should_pause_for_detected_speech(true, true, true));
    }

    #[test]
    fn barge_in_routes_plain_speech_directly_without_a_wake_phrase() {
        assert_eq!(
            direct_barge_in_intent(
                "change the grass to light brown",
                VoiceIntent::NotAddressed,
                true,
            ),
            VoiceIntent::Prompt("change the grass to light brown".into())
        );
    }

    #[test]
    fn ambient_speech_remains_ignored_when_playback_was_not_interrupted() {
        assert_eq!(
            direct_barge_in_intent("pass me the water", VoiceIntent::NotAddressed, false),
            VoiceIntent::NotAddressed
        );
    }

    #[test]
    fn barge_in_preserves_safety_controls() {
        assert_eq!(
            direct_barge_in_intent("cancel", VoiceIntent::Cancel, true),
            VoiceIntent::Cancel
        );
        assert_eq!(
            direct_barge_in_intent("stop speaking", VoiceIntent::StopSpeaking, true),
            VoiceIntent::StopSpeaking
        );
    }

    #[test]
    fn execution_prompt_separates_ssh_from_other_capabilities() {
        let prompt = execution_worker_prompt(
            "Deploy the backend.",
            false,
            WorkerEngine::Codex,
            SandboxMode::WorkspaceWrite,
            true,
            "build-host",
            "/home/developer/projects/example-app",
        );
        assert!(prompt.contains("SSH transport, local device tooling, remote tooling"));
        assert!(
            prompt.contains(
                "already launched this exact process on `build-host` through key-only SSH"
            )
        );
        assert!(prompt.contains("Do not SSH to `build-host` again"));
        assert!(
            prompt
                .contains("Run the user's commands, tool calls, edits, builds, and tests directly")
        );
        assert!(prompt.contains("Mac is only capturing speech"));
        assert!(prompt.contains("never describe one as an SSH failure unless SSH itself failed"));
        assert!(prompt.contains("exhaust safe in-scope alternatives"));
        assert!(prompt.contains("Deploy the backend."));
    }

    #[test]
    fn local_worker_prompt_keeps_visual_qa_on_the_mac() {
        let prompt = execution_worker_prompt(
            "QA the profile screen.",
            true,
            WorkerEngine::Codex,
            SandboxMode::DangerFullAccess,
            true,
            "build-host",
            "/home/developer/projects/example-app",
        );
        assert!(prompt.contains("user explicitly enabled Full Mac access"));
        assert!(prompt.contains("Mac-local ADB installation"));
        assert!(prompt.contains("Never route Mac-only visual QA through the remote SSH host"));
        assert!(prompt.contains("`build-host`"));
        assert!(prompt.contains("/home/developer/projects/example-app"));
        assert!(prompt.contains("working directory as launch context"));
        assert!(prompt.contains("create it in its own sibling directory"));
        assert!(prompt.contains("never repurpose another app"));
    }

    #[test]
    fn claude_worker_prompt_identifies_opus_xhigh_and_full_mac_execution() {
        let prompt = execution_worker_prompt(
            "Inspect the simulator.",
            true,
            WorkerEngine::Claude,
            SandboxMode::DangerFullAccess,
            false,
            "build-host",
            "/home/developer/projects/example-app",
        );
        assert!(prompt.contains("Mac-local Claude Code execution worker"));
        assert!(prompt.contains("user explicitly enabled Full Mac access"));
        assert!(prompt.contains("Mac-local ADB installation"));
    }

    #[test]
    fn direct_voice_updates_identify_the_execution_host() {
        assert_eq!(
            attributed_execution_update(false, "I am running the tests."),
            "SSH worker update: I am running the tests."
        );
        assert_eq!(
            attributed_execution_update(true, "I opened the simulator."),
            "Mac worker update: I opened the simulator."
        );
    }

    #[test]
    fn named_worker_request_never_steers_the_other_engine() {
        assert!(!targets_different_worker(
            Some(WorkerEngine::Codex),
            WorkerEngine::Codex
        ));
        assert!(!targets_different_worker(None, WorkerEngine::Claude));
        assert!(targets_different_worker(
            Some(WorkerEngine::Claude),
            WorkerEngine::Codex
        ));
        assert!(targets_different_worker(
            Some(WorkerEngine::Codex),
            WorkerEngine::Claude
        ));
    }

    #[test]
    fn only_ssh_shaped_errors_are_transport_failures() {
        assert!(is_ssh_transport_failure(
            "ssh: Could not resolve hostname build-host: nodename nor servname provided"
        ));
        assert!(is_ssh_transport_failure(
            "developer@host: Permission denied (publickey,password)."
        ));
        assert!(!is_ssh_transport_failure("SUPABASE_ACCESS_TOKEN is unset"));
        assert!(!is_ssh_transport_failure(
            "permission denied while trying to connect to the Docker daemon socket"
        ));
    }

    #[test]
    fn connectivity_errors_retry_but_configuration_errors_do_not() {
        assert!(is_transient_connectivity_failure(
            "error sending request for url (https://chatgpt.com/backend-api/codex/responses)"
        ));
        assert!(is_transient_connectivity_failure(
            "stream disconnected before completion"
        ));
        assert!(is_transient_connectivity_failure(
            "Codex app-server stream unresponsive after 600 seconds without events"
        ));
        assert!(is_transient_connectivity_failure(
            "The Codex service route failed sustained probes while the worker was silent"
        ));
        assert!(is_transient_connectivity_failure(
            "Server overloaded; retry later."
        ));
        assert!(is_transient_connectivity_failure("Reconnecting... 4/5"));
        assert!(!is_transient_connectivity_failure(
            "model gpt-example is not available"
        ));
        assert!(!is_transient_connectivity_failure(
            "approval policy rejected the command"
        ));
    }

    #[test]
    fn reconnect_delay_grows_and_caps_at_thirty_seconds() {
        let delays = (1..=8).map(reconnect_delay).collect::<Vec<_>>();
        assert!(delays.windows(2).take(5).all(|pair| pair[1] > pair[0]));
        assert_eq!(delays[6], MAX_RECONNECT_DELAY);
        assert_eq!(delays[7], MAX_RECONNECT_DELAY);
    }

    #[test]
    fn reconnect_prompt_preserves_additions_and_guards_side_effects() {
        let request = PendingRequest {
            prompt: "Run the release QA".into(),
            additions: vec!["Also test the profile screen".into()],
        };
        let prompt = reconnect_prompt(&request, true, WorkerEngine::Codex);
        assert!(prompt.contains("persisted Codex thread"));
        assert!(prompt.contains("do not repeat completed commands"));
        assert!(prompt.contains("Run the release QA"));
        assert!(prompt.contains("Also test the profile screen"));
    }

    #[test]
    fn parallel_follow_up_resumes_the_same_worker_without_repeating_finished_work() {
        let request = PendingRequest {
            prompt: "Build the Worm Flayer".into(),
            additions: vec!["Use reference four".into()],
        };
        let resumed = parallel_follow_up_prompt(&request, true);
        assert!(resumed.contains("existing worker session"));
        assert!(resumed.contains("do not repeat work that is already complete"));
        assert!(resumed.contains("Use reference four"));
        assert!(!resumed.contains("Build the Worm Flayer"));

        let fresh = parallel_follow_up_prompt(&request, false);
        assert!(fresh.contains("ORIGINAL REQUEST:\nBuild the Worm Flayer"));
        assert!(fresh.contains("Use reference four"));
    }

    #[test]
    fn contextual_phrases_are_recognized_as_follow_ups() {
        for prompt in [
            "also check the profile screen",
            "by the way run the security test",
            "/btw include the migration",
            "can you also inspect the logs",
            "that should look like live energy",
            "this needs more contrast",
            "it should stay in the new project",
            "continue with the build",
            "go ahead and run it",
            "yes, make that change",
        ] {
            assert!(is_follow_up_request(prompt), "{prompt}");
        }
        for prompt in [
            "run visual QA on the profile",
            "deploy the backend",
            "do a separate security audit",
        ] {
            assert!(!is_follow_up_request(prompt), "{prompt}");
        }
    }

    #[test]
    fn explicit_parallel_prefix_is_removed_before_worker_launch() {
        assert_eq!(
            strip_parallel_request_prefix("in parallel, run the mobile tests"),
            ("run the mobile tests", true)
        );
        assert_eq!(
            strip_parallel_request_prefix("start another worker to inspect the logs"),
            ("inspect the logs", true)
        );
        assert_eq!(
            strip_parallel_request_prefix("separate task: audit the API"),
            ("audit the API", true)
        );
        assert_eq!(
            strip_parallel_request_prefix("fire up a subagent to run visual QA"),
            ("run visual QA", true)
        );
        assert_eq!(
            strip_parallel_request_prefix("also inspect the logs"),
            ("also inspect the logs", false)
        );
        assert_eq!(
            strip_parallel_request_prefix("run Profile QA"),
            ("run Profile QA", false)
        );
    }

    #[test]
    fn explicit_worker_numbers_route_without_guessing() {
        for (prompt, expected) in [
            ("worker one, keep touching up grassland", 1),
            ("Worker 2, reference four", 2),
            ("send this to the third worker", 3),
            ("the primary worker should keep the brown stage", 1),
            ("worker number two should handle Worm Flayer", 2),
        ] {
            assert_eq!(referenced_worker(prompt), WorkerReference::Label(expected));
        }
        assert_eq!(
            referenced_worker("also use reference four"),
            WorkerReference::None
        );
    }

    #[test]
    fn conflicting_worker_references_require_clarification() {
        assert_eq!(
            referenced_worker("worker one should use worker two's reference"),
            WorkerReference::Ambiguous
        );
    }

    #[test]
    fn clarification_reply_can_be_only_a_worker_selector() {
        for prompt in [
            "worker 2",
            "the second worker",
            "primary worker please",
            "two",
            "worker too",
        ] {
            assert!(is_worker_selection_only(prompt), "{prompt}");
            assert_eq!(
                clarification_worker_reference(prompt),
                if prompt.contains("primary") {
                    WorkerReference::Label(1)
                } else {
                    WorkerReference::Label(2)
                }
            );
        }
        assert!(!is_worker_selection_only("worker 2, reference four"));
    }

    #[test]
    fn clarification_question_names_tasks_and_confirms_request_is_held() {
        let question = worker_clarification_question(&[
            (1, "touch up the grassland biome".into()),
            (2, "build the Worm Flayer".into()),
        ]);
        assert!(question.contains("worker 1 — touch up the grassland biome"));
        assert!(question.contains("worker 2 — build the Worm Flayer"));
        assert!(question.contains("not forwarded it yet"));
    }

    #[test]
    fn active_requests_default_to_primary_unless_parallel_is_explicit() {
        assert!(!should_launch_parallel(false, None, WorkerEngine::Codex));
        assert!(!should_launch_parallel(
            false,
            Some(WorkerEngine::Codex),
            WorkerEngine::Codex
        ));
        assert!(should_launch_parallel(true, None, WorkerEngine::Codex));
        assert!(should_launch_parallel(
            false,
            Some(WorkerEngine::Claude),
            WorkerEngine::Codex
        ));
    }

    #[test]
    fn new_project_requests_start_fresh_but_follow_ups_keep_context() {
        for prompt in [
            "Create a new project for an audio visualizer",
            "I want to create a new project for an audio visualizer",
            "Build a new app for music playback",
            "Start a new application for the car",
        ] {
            assert!(should_start_fresh_session(prompt), "{prompt}");
        }
        for prompt in [
            "continue the current task",
            "that should look like live energy",
            "also add a waveform",
            "/btw keep it native",
            "fix the current app",
        ] {
            assert!(!should_start_fresh_session(prompt), "{prompt}");
        }
    }

    #[test]
    fn parallel_worker_prompt_protects_concurrent_workspace_changes() {
        let prompt = parallel_worker_prompt(2, "USER TASK:\nRun QA");
        assert!(prompt.contains("independent worker session"));
        assert!(prompt.contains("working directory is only launch context"));
        assert!(prompt.contains("verify the user's intended project root"));
        assert!(prompt.contains("preserve unrelated concurrent changes"));
        assert!(prompt.contains("worker 2"));
    }

    #[test]
    fn spoken_mac_workspace_decodes_path_words() {
        assert_eq!(
            decode_spoken_path("slash Users slash example slash sample dash project"),
            PathBuf::from("/Users/example/sample-project")
        );
    }

    #[test]
    fn literal_mac_workspace_keeps_path_punctuation() {
        assert_eq!(
            decode_spoken_path("/Users/example/sample-project"),
            PathBuf::from("/Users/example/sample-project")
        );
    }
}
