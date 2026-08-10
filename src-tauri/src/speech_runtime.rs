#![cfg_attr(not(target_os = "macos"), allow(dead_code))]
#![allow(
    clippy::needless_pass_by_value,
    reason = "the runtime owns both channel endpoints on its dedicated OS thread"
)]

#[cfg(target_os = "macos")]
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
#[cfg(target_os = "macos")]
use std::time::{Duration, Instant};

use tokio::sync::mpsc as tokio_mpsc;

#[cfg(target_os = "macos")]
use crate::neural_tts::{NeuralTts, NeuralTtsEvent};
#[cfg(target_os = "macos")]
use roadie_core::{VoiceIntent, parse_voice_intent};
#[cfg(target_os = "macos")]
use silero_vad_crs::SileroVad;

#[cfg(target_os = "macos")]
const WHISPER_SAMPLE_RATE: u32 = 16_000;
#[cfg(target_os = "macos")]
const PRE_ROLL_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize * 2 / 5;
const PLAYBACK_PRE_ROLL_GUARD_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize * 4 / 100;
#[cfg(target_os = "macos")]
const PLAYBACK_PROBE_CONFIRM_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize / 25;
#[cfg(target_os = "macos")]
const PLAYBACK_PROBE_ECHO_TAIL_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize * 4 / 100;
#[cfg(target_os = "macos")]
const PLAYBACK_WARMUP_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize * 2 / 25;
#[cfg(target_os = "macos")]
const MIN_UTTERANCE_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize * 3 / 10;
#[cfg(target_os = "macos")]
const MAX_UTTERANCE_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize * 30;
#[cfg(target_os = "macos")]
const PLAYBACK_CONFIRMATION_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize * 3 / 2;
#[cfg(target_os = "macos")]
const VAD_CALIBRATION_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize / 2;
#[cfg(target_os = "macos")]
const VAD_START_CONFIRM_SAMPLES: usize = WHISPER_SAMPLE_RATE as usize / 5;
#[cfg(target_os = "macos")]
const VAD_SPEECH_LOW_PASS_ALPHA: f32 = 0.38;
#[cfg(target_os = "macos")]
const VAD_MIN_VOICE_PERIODICITY: f32 = 0.62;
#[cfg(target_os = "macos")]
const VAD_MIN_SPEECH_ZERO_CROSSING_RATE: f32 = 0.008;
#[cfg(target_os = "macos")]
const VAD_MIN_SPECTRAL_DETAIL_RATIO: f32 = 0.12;
#[cfg(target_os = "macos")]
const VAD_MAX_SPEECH_SPECTRAL_FLATNESS: f32 = 0.18;
#[cfg(target_os = "macos")]
const WHISPER_NO_SPEECH_REJECT: f32 = 0.88;
#[cfg(target_os = "macos")]
const WHISPER_SHORT_TRANSCRIPT_MAX_WORDS: usize = 2;
#[cfg(target_os = "macos")]
const WHISPER_MIN_SHORT_TOKEN_PROBABILITY: f32 = 0.55;
#[cfg(target_os = "macos")]
const WHISPER_MIN_WAKE_TOKEN_PROBABILITY: f32 = 0.05;
#[cfg(target_os = "macos")]
const PLAYBACK_PROBE_PAUSE_TIMEOUT: Duration = Duration::from_millis(180);
#[cfg(target_os = "macos")]
const PLAYBACK_PROBE_CONFIRM_TIMEOUT: Duration = Duration::from_millis(360);

#[derive(Debug)]
pub enum SpeechCommand {
    StartListening {
        locale: String,
        silence_ms: u64,
        wake_phrase: String,
        voice_isolation: bool,
    },
    StopListening,
    Speak {
        text: String,
        rate: f32,
    },
    PauseSpeaking,
    StopSpeaking,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedSpeechSource {
    User,
    UserOverRoadiePlayback,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackProbePhase {
    AwaitingPause,
    ConfirmingUser,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy)]
struct PlaybackProbe {
    phase: PlaybackProbePhase,
    started_at: Instant,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PlaybackProbeRequest {
    #[default]
    Idle,
    Requested,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum SpeechConfirmationMode {
    #[default]
    Normal,
    AfterPlaybackPause,
}

#[derive(Debug, Clone)]
pub enum SpeechEngineEvent {
    ModelLoading,
    ModelReady,
    VoiceModelLoading(String),
    VoiceModelReady(String),
    VoiceGenerating {
        tokens: usize,
        elapsed_ms: u64,
    },
    Authorization(String),
    ListeningStarted(u64),
    ListeningStopped,
    MicrophoneRecovering(String),
    AudioLevel {
        generation: u64,
        level: f32,
    },
    SpeechDetected {
        generation: u64,
        source: DetectedSpeechSource,
    },
    Transcribing {
        generation: u64,
        audio_ms: u64,
    },
    Transcript {
        generation: u64,
        transcript: String,
        is_final: bool,
    },
    UtteranceIgnored(u64),
    SpeakingStarted,
    SpeakingPaused,
    SpeakingResumed,
    SpeakingFinished,
    SpeakingCancelled,
    Error(String),
}

#[derive(Clone)]
pub struct SpeechHandle {
    sender: mpsc::Sender<SpeechCommand>,
    worker: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}

impl SpeechHandle {
    pub fn send(&self, command: SpeechCommand) {
        let _ = self.sender.send(command);
    }

    pub fn shutdown_and_join(&self) {
        let _ = self.sender.send(SpeechCommand::Shutdown);
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }
}

pub fn spawn(
    model_path: PathBuf,
) -> (
    SpeechHandle,
    tokio_mpsc::UnboundedReceiver<SpeechEngineEvent>,
) {
    let (command_tx, command_rx) = mpsc::channel();
    let (event_tx, event_rx) = tokio_mpsc::unbounded_channel();
    let worker = std::thread::Builder::new()
        .name("codex-roadie-speech".into())
        .spawn(move || run(command_rx, event_tx, model_path))
        .expect("failed to start speech runtime thread");
    (
        SpeechHandle {
            sender: command_tx,
            worker: Arc::new(Mutex::new(Some(worker))),
        },
        event_rx,
    )
}

#[cfg(target_os = "macos")]
#[allow(clippy::too_many_lines)]
fn run(
    receiver: mpsc::Receiver<SpeechCommand>,
    events: tokio_mpsc::UnboundedSender<SpeechEngineEvent>,
    model_path: PathBuf,
) {
    use avspeechsynthesizer::prelude::*;
    use whisper_rs::{WhisperContext, WhisperContextParameters};

    let _ = events.send(SpeechEngineEvent::ModelLoading);
    let mut context_parameters = WhisperContextParameters::default();
    context_parameters
        .use_gpu(true)
        .flash_attn(true)
        .gpu_device(0);
    let mut decoder = model_path
        .to_str()
        .ok_or_else(|| "The Whisper model path is not valid UTF-8".to_owned())
        .and_then(|path| {
            if model_path.is_file() {
                WhisperContext::new_with_params(path, context_parameters)
                    .map_err(|error| format!("Could not load the local Whisper model: {error}"))
            } else {
                Err(format!(
                    "Local Whisper model is missing at {}. Run scripts/install-whisper-model.sh.",
                    model_path.display()
                ))
            }
        })
        .and_then(|context| {
            context
                .create_state()
                .map_err(|error| format!("Could not initialize the Metal Whisper decoder: {error}"))
        });
    match &decoder {
        Ok(_) => {
            let _ = events.send(SpeechEngineEvent::Authorization(
                "Local Whisper large-v3-turbo-q5_0 loaded with Metal GPU and flash attention"
                    .into(),
            ));
        }
        Err(error) => {
            let _ = events.send(SpeechEngineEvent::Error(error.clone()));
        }
    }
    if let Ok(state) = &mut decoder {
        let _ = events.send(SpeechEngineEvent::Authorization(
            "Pre-warming the persistent Metal decoder".into(),
        ));
        let warmup_audio = vec![0.0; WHISPER_SAMPLE_RATE as usize];
        let _ = transcribe(state, &warmup_audio, "en", "hey codex");
        let _ = events.send(SpeechEngineEvent::ModelReady);
    }

    let (audio_tx, audio_rx) = mpsc::sync_channel::<Vec<f32>>(128);
    let (microphone_error_tx, microphone_error_rx) = mpsc::sync_channel::<String>(4);
    let mut microphone: Option<cpal::Stream> = None;
    let mut microphone_restart: Option<(Instant, u8, String)> = None;
    let mut resampler: Option<StreamingResampler> = None;
    let mut detector = UtteranceDetector::new(1_100);
    let mut generation = 0_u64;
    let mut listening = false;
    let mut playback_probe: Option<PlaybackProbe> = None;
    let mut playback_probe_armed = true;
    let mut deferred_neural_speech: Option<(String, f32)> = None;
    let mut last_level_emit = Instant::now();
    let mut language = String::from("en");
    let mut wake_phrase = String::from("hey codex");
    let mut synthesizer = match SpeechSynthesizer::new() {
        Ok(mut synthesizer) => {
            let event_sender = events.clone();
            synthesizer.set_event_handler(move |event| match event {
                SpeechEvent::DidStart(_) => {
                    let _ = event_sender.send(SpeechEngineEvent::SpeakingStarted);
                }
                SpeechEvent::DidFinish(_) => {
                    let _ = event_sender.send(SpeechEngineEvent::SpeakingFinished);
                }
                SpeechEvent::DidPause(_) => {
                    let _ = event_sender.send(SpeechEngineEvent::SpeakingPaused);
                }
                SpeechEvent::DidContinue(_) => {
                    let _ = event_sender.send(SpeechEngineEvent::SpeakingResumed);
                }
                SpeechEvent::DidCancel(_) => {
                    let _ = event_sender.send(SpeechEngineEvent::SpeakingCancelled);
                }
                _ => {}
            });
            Some(synthesizer)
        }
        Err(error) => {
            let _ = events.send(SpeechEngineEvent::Error(format!(
                "Text-to-speech failed to initialize: {error}"
            )));
            None
        }
    };
    let mut neural_tts = match NeuralTts::spawn() {
        Ok(neural_tts) => Some(neural_tts),
        Err(error) => {
            let _ = events.send(SpeechEngineEvent::Authorization(format!(
                "{error}. Using Apple speech until the neural helper is available."
            )));
            None
        }
    };

    loop {
        if let Some(neural_tts) = neural_tts.as_mut() {
            drain_neural_events(neural_tts, synthesizer.as_ref(), &events);
        }
        if let Some(neural_tts) = neural_tts.as_mut()
            && neural_tts.is_ready()
            && let Some((text, rate)) = deferred_neural_speech.take()
        {
            if let Some(synthesizer) = synthesizer.as_ref()
                && synthesizer.is_speaking()
            {
                let _ = synthesizer.stop_speaking(SpeechBoundary::Immediate);
            }
            if let Err(error) = neural_tts.speak(text, rate) {
                let _ = events.send(SpeechEngineEvent::Error(format!(
                    "Could not start the fixed Kokoro voice: {error}"
                )));
            }
        }
        if listening {
            let mut latest_microphone_error = None;
            while let Ok(error) = microphone_error_rx.try_recv() {
                latest_microphone_error = Some(error);
            }
            if let Some(error) = latest_microphone_error {
                microphone.take();
                resampler.take();
                while audio_rx.try_recv().is_ok() {}
                detector.reset();
                microphone_restart = Some((
                    Instant::now() + microphone_reconnect_delay(1),
                    1,
                    error.clone(),
                ));
                let _ = events.send(SpeechEngineEvent::MicrophoneRecovering(format!(
                    "{error} • reopening the current audio route"
                )));
            }
            if let Some((deadline, attempt, previous_error)) = microphone_restart.clone()
                && deadline <= Instant::now()
            {
                match start_microphone_stream(audio_tx.clone(), microphone_error_tx.clone()) {
                    Ok((stream, sample_rate)) => {
                        microphone = Some(stream);
                        resampler = Some(StreamingResampler::new(sample_rate));
                        detector.reset();
                        microphone_restart = None;
                        let _ = events.send(SpeechEngineEvent::ListeningStarted(generation));
                        let _ = events.send(SpeechEngineEvent::Authorization(format!(
                            "Microphone recovered at {sample_rate} Hz after audio-route change"
                        )));
                    }
                    Err(error) if attempt < 5 => {
                        let next_attempt = attempt + 1;
                        let delay = microphone_reconnect_delay(next_attempt);
                        microphone_restart =
                            Some((Instant::now() + delay, next_attempt, error.clone()));
                        let _ = events.send(SpeechEngineEvent::MicrophoneRecovering(format!(
                            "Audio route is still changing • retry {next_attempt}/5 in {} ms",
                            delay.as_millis()
                        )));
                    }
                    Err(error) => {
                        listening = false;
                        microphone_restart = None;
                        let _ = events.send(SpeechEngineEvent::Error(format!(
                            "Could not recover the microphone after an audio-route change: {error}. Previous error: {previous_error}"
                        )));
                    }
                }
            }
        }
        match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(SpeechCommand::StartListening {
                locale,
                silence_ms,
                wake_phrase: configured_wake_phrase,
                voice_isolation,
            }) => {
                if decoder.is_err() {
                    if let Err(error) = &decoder {
                        let _ = events.send(SpeechEngineEvent::Error(error.clone()));
                    }
                    continue;
                }
                let opening_microphone = microphone.is_none();
                if opening_microphone {
                    match start_microphone_stream(audio_tx.clone(), microphone_error_tx.clone()) {
                        Ok((stream, sample_rate)) => {
                            microphone = Some(stream);
                            resampler = Some(StreamingResampler::new(sample_rate));
                        }
                        Err(error) => {
                            let _ = events.send(SpeechEngineEvent::Error(error));
                            continue;
                        }
                    }
                }
                while audio_rx.try_recv().is_ok() {}
                if let Some(resampler) = resampler.as_mut() {
                    resampler.reset();
                }
                detector.rearm(silence_ms);
                detector.set_voice_isolation(voice_isolation);
                language = whisper_language(&locale);
                wake_phrase = configured_wake_phrase;
                generation = generation.wrapping_add(1);
                listening = true;
                playback_probe = None;
                last_level_emit = Instant::now();
                let _ = events.send(SpeechEngineEvent::ListeningStarted(generation));
            }
            Ok(SpeechCommand::StopListening) => {
                listening = false;
                playback_probe = None;
                microphone.take();
                resampler.take();
                microphone_restart = None;
                while microphone_error_rx.try_recv().is_ok() {}
                while audio_rx.try_recv().is_ok() {}
                detector.reset();
                let _ = events.send(SpeechEngineEvent::ListeningStopped);
            }
            Ok(SpeechCommand::Speak { text, rate }) => {
                playback_probe = None;
                playback_probe_armed = true;
                let text = normalize_technical_pronunciations(&text);
                if let Some(synthesizer) = synthesizer.as_ref()
                    && synthesizer.is_speaking()
                {
                    let _ = synthesizer.stop_speaking(SpeechBoundary::Immediate);
                }
                if let Some(neural_tts) = neural_tts.as_mut() {
                    if neural_tts.is_ready() {
                        deferred_neural_speech = None;
                        if let Err(error) = neural_tts.speak(text, rate) {
                            let _ = events.send(SpeechEngineEvent::Error(format!(
                                "Could not start the fixed Kokoro voice: {error}"
                            )));
                        }
                    } else {
                        // Keep only the newest useful update while the fixed voice warms. This
                        // prevents an Apple voice from overlapping Kokoro as soon as it is ready.
                        deferred_neural_speech = Some((text, rate));
                    }
                } else {
                    speak_native(synthesizer.as_ref(), text, rate, &events);
                }
            }
            Ok(SpeechCommand::PauseSpeaking) => {
                playback_probe = None;
                if let Some(neural_tts) = neural_tts.as_mut() {
                    let _ = neural_tts.pause();
                }
                if let Some(synthesizer) = synthesizer.as_ref()
                    && synthesizer.is_speaking()
                    && !synthesizer.is_paused()
                {
                    let _ = synthesizer.pause_speaking(SpeechBoundary::Immediate);
                }
            }
            Ok(SpeechCommand::StopSpeaking) => {
                playback_probe = None;
                deferred_neural_speech = None;
                if let Some(neural_tts) = neural_tts.as_mut() {
                    let _ = neural_tts.stop();
                }
                if let Some(synthesizer) = synthesizer.as_ref() {
                    let _ = synthesizer.stop_speaking(SpeechBoundary::Immediate);
                }
            }
            Ok(SpeechCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        if let Some(mut probe) = playback_probe {
            match probe.phase {
                PlaybackProbePhase::AwaitingPause
                    if playback_is_paused(neural_tts.as_ref(), synthesizer.as_ref()) =>
                {
                    detector.begin_playback_probe_confirmation();
                    probe.phase = PlaybackProbePhase::ConfirmingUser;
                    probe.started_at = Instant::now();
                    playback_probe = Some(probe);
                }
                PlaybackProbePhase::AwaitingPause
                    if probe.started_at.elapsed() >= PLAYBACK_PROBE_PAUSE_TIMEOUT =>
                {
                    stop_playback(&mut neural_tts, synthesizer.as_ref());
                    detector.begin_playback_probe_confirmation();
                    probe.phase = PlaybackProbePhase::ConfirmingUser;
                    probe.started_at = Instant::now();
                    playback_probe = Some(probe);
                }
                PlaybackProbePhase::ConfirmingUser
                    if probe.started_at.elapsed() >= PLAYBACK_PROBE_CONFIRM_TIMEOUT
                        && !detector.speaking =>
                {
                    detector.cancel_playback_probe();
                    resume_playback(&mut neural_tts, synthesizer.as_ref());
                    playback_probe = None;
                }
                _ => {}
            }
        }

        while listening {
            let Ok(input) = audio_rx.try_recv() else {
                break;
            };
            let Some(resampler) = resampler.as_mut() else {
                break;
            };
            let samples = resampler.push(&input);
            if last_level_emit.elapsed() >= Duration::from_millis(100) {
                last_level_emit = Instant::now();
                let _ = events.send(SpeechEngineEvent::AudioLevel {
                    generation,
                    level: normalized_audio_level(&samples),
                });
            }
            let playback_active = playback_is_active(neural_tts.as_ref(), synthesizer.as_ref());
            let detector_playback_active = playback_active
                && !playback_probe
                    .is_some_and(|probe| probe.phase == PlaybackProbePhase::ConfirmingUser);
            let was_speaking = detector.speaking;
            let completed = detector.push(&samples, detector_playback_active);
            let probe_requested = detector.take_playback_probe_request();
            if should_begin_playback_probe(
                playback_probe,
                playback_active,
                playback_probe_armed,
                probe_requested,
            ) {
                playback_probe_armed = false;
                pause_playback(&mut neural_tts, synthesizer.as_ref());
                playback_probe = Some(PlaybackProbe {
                    phase: PlaybackProbePhase::AwaitingPause,
                    started_at: Instant::now(),
                });
            }
            if !was_speaking && detector.speaking {
                let interrupted_playback = playback_active || playback_probe.is_some();
                playback_probe = None;
                let _ = events.send(SpeechEngineEvent::SpeechDetected {
                    generation,
                    source: if interrupted_playback {
                        DetectedSpeechSource::UserOverRoadiePlayback
                    } else {
                        DetectedSpeechSource::User
                    },
                });
            }
            if let Some(utterance) = completed {
                listening = false;
                let sample_count = u64::try_from(utterance.len()).unwrap_or(u64::MAX);
                let _ = events.send(SpeechEngineEvent::Transcribing {
                    generation,
                    audio_ms: sample_count.saturating_mul(1_000) / u64::from(WHISPER_SAMPLE_RATE),
                });
                if let Ok(state) = &mut decoder {
                    match transcribe(state, &utterance, &language, &wake_phrase) {
                        Ok(transcript) if !transcript.is_empty() => {
                            let _ = events.send(SpeechEngineEvent::Transcript {
                                generation,
                                transcript,
                                is_final: true,
                            });
                        }
                        Ok(_) => {
                            while audio_rx.try_recv().is_ok() {}
                            resampler.reset();
                            detector.reset();
                            listening = true;
                            let _ = events.send(SpeechEngineEvent::UtteranceIgnored(generation));
                            let _ = events.send(SpeechEngineEvent::Authorization(
                                "Whisper ignored an empty utterance and kept listening".into(),
                            ));
                        }
                        Err(error) => {
                            let _ = events.send(SpeechEngineEvent::Error(error));
                        }
                    }
                }
                break;
            }
        }

        if let Some(synthesizer) = synthesizer.as_ref() {
            synthesizer.pump_run_loop(Duration::from_millis(5));
        }
    }
    microphone.take();
    if let Some(neural_tts) = neural_tts.as_mut() {
        neural_tts.shutdown();
    }
    synthesizer.take();
}

#[cfg(target_os = "macos")]
fn drain_neural_events(
    neural_tts: &mut NeuralTts,
    synthesizer: Option<&avspeechsynthesizer::SpeechSynthesizer>,
    events: &tokio_mpsc::UnboundedSender<SpeechEngineEvent>,
) {
    while let Some(event) = neural_tts.poll() {
        match event {
            NeuralTtsEvent::Loading(detail) | NeuralTtsEvent::Warming(detail) => {
                let _ = events.send(SpeechEngineEvent::VoiceModelLoading(detail));
            }
            NeuralTtsEvent::Ready(detail) => {
                let _ = events.send(SpeechEngineEvent::VoiceModelReady(detail));
            }
            NeuralTtsEvent::Generating {
                id,
                tokens,
                elapsed_ms,
            } => {
                let _ = id;
                let _ = events.send(SpeechEngineEvent::VoiceGenerating { tokens, elapsed_ms });
            }
            NeuralTtsEvent::Generated {
                id,
                detail,
                tokens,
                elapsed_ms,
            } => {
                let _ = (id, tokens, elapsed_ms);
                let _ = events.send(SpeechEngineEvent::Authorization(format!(
                    "Kokoro: {detail}"
                )));
            }
            NeuralTtsEvent::Started(id) => {
                let _ = id;
                let _ = events.send(SpeechEngineEvent::SpeakingStarted);
            }
            NeuralTtsEvent::Paused(id) => {
                let _ = id;
                let _ = events.send(SpeechEngineEvent::SpeakingPaused);
            }
            NeuralTtsEvent::Resumed(id) => {
                let _ = id;
                let _ = events.send(SpeechEngineEvent::SpeakingResumed);
            }
            NeuralTtsEvent::Finished(id) => {
                let _ = id;
                let _ = events.send(SpeechEngineEvent::SpeakingFinished);
            }
            NeuralTtsEvent::Cancelled(id) => {
                let _ = id;
                let _ = events.send(SpeechEngineEvent::SpeakingCancelled);
            }
            NeuralTtsEvent::Failed { id, detail } => {
                let fallback = neural_tts.take_pending(id);
                let _ = events.send(SpeechEngineEvent::Authorization(format!(
                    "{detail}. Falling back to Apple speech."
                )));
                if let Some(fallback) = fallback {
                    speak_native(synthesizer, fallback.text, fallback.rate, events);
                }
            }
            NeuralTtsEvent::Log(detail) => {
                if !detail.is_empty() {
                    let _ = events.send(SpeechEngineEvent::VoiceModelLoading(detail));
                }
            }
            NeuralTtsEvent::Stopped(detail) => {
                let fallback = neural_tts.take_pending(None);
                let _ = events.send(SpeechEngineEvent::Authorization(detail));
                if let Some(fallback) = fallback {
                    speak_native(synthesizer, fallback.text, fallback.rate, events);
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn should_begin_playback_probe(
    probe: Option<PlaybackProbe>,
    playback_active: bool,
    probe_armed: bool,
    probe_requested: bool,
) -> bool {
    probe.is_none() && playback_active && probe_armed && probe_requested
}

#[cfg(target_os = "macos")]
fn playback_is_active(
    neural_tts: Option<&NeuralTts>,
    synthesizer: Option<&avspeechsynthesizer::SpeechSynthesizer>,
) -> bool {
    neural_tts.is_some_and(NeuralTts::is_playing)
        || synthesizer
            .is_some_and(|synthesizer| synthesizer.is_speaking() && !synthesizer.is_paused())
}

#[cfg(target_os = "macos")]
fn playback_is_paused(
    neural_tts: Option<&NeuralTts>,
    synthesizer: Option<&avspeechsynthesizer::SpeechSynthesizer>,
) -> bool {
    neural_tts.is_some_and(NeuralTts::is_paused)
        || synthesizer
            .is_some_and(|synthesizer| synthesizer.is_speaking() && synthesizer.is_paused())
}

#[cfg(target_os = "macos")]
fn pause_playback(
    neural_tts: &mut Option<NeuralTts>,
    synthesizer: Option<&avspeechsynthesizer::SpeechSynthesizer>,
) {
    if let Some(neural_tts) = neural_tts.as_mut() {
        let _ = neural_tts.pause();
    }
    if let Some(synthesizer) = synthesizer
        && synthesizer.is_speaking()
        && !synthesizer.is_paused()
    {
        let _ = synthesizer.pause_speaking(avspeechsynthesizer::SpeechBoundary::Immediate);
    }
}

#[cfg(target_os = "macos")]
fn resume_playback(
    neural_tts: &mut Option<NeuralTts>,
    synthesizer: Option<&avspeechsynthesizer::SpeechSynthesizer>,
) {
    if let Some(neural_tts) = neural_tts.as_mut()
        && neural_tts.is_paused()
    {
        let _ = neural_tts.resume();
    }
    if let Some(synthesizer) = synthesizer
        && synthesizer.is_speaking()
        && synthesizer.is_paused()
    {
        let _ = synthesizer.continue_speaking();
    }
}

#[cfg(target_os = "macos")]
fn stop_playback(
    neural_tts: &mut Option<NeuralTts>,
    synthesizer: Option<&avspeechsynthesizer::SpeechSynthesizer>,
) {
    if let Some(neural_tts) = neural_tts.as_mut() {
        let _ = neural_tts.stop();
    }
    if let Some(synthesizer) = synthesizer {
        let _ = synthesizer.stop_speaking(avspeechsynthesizer::SpeechBoundary::Immediate);
    }
}

#[cfg(target_os = "macos")]
fn speak_native(
    synthesizer: Option<&avspeechsynthesizer::SpeechSynthesizer>,
    text: String,
    rate: f32,
    events: &tokio_mpsc::UnboundedSender<SpeechEngineEvent>,
) {
    use avspeechsynthesizer::prelude::*;

    let Some(synthesizer) = synthesizer else {
        let _ = events.send(SpeechEngineEvent::Error(
            "Neither Kokoro nor Apple text-to-speech is available".into(),
        ));
        return;
    };
    let utterance = SpeechUtterance::new(text).with_rate(rate.clamp(0.1, 1.0));
    if let Err(error) = synthesizer.speak(&utterance) {
        let _ = events.send(SpeechEngineEvent::Error(format!(
            "Could not speak response: {error}"
        )));
    }
}

#[cfg(target_os = "macos")]
fn normalize_technical_pronunciations(text: &str) -> String {
    const PRONUNCIATIONS: [(&str, &str); 35] = [
        ("Supabase", "Supa base"),
        ("TestFlight", "Test Flight"),
        ("GitHub", "Git Hub"),
        ("macOS", "mac O S"),
        ("HTTPS", "H T T P S"),
        ("HTTP", "H T T P"),
        ("OpenAI", "Open A I"),
        ("pgTAP", "P G tap"),
        ("AWS", "A W S"),
        ("ECS", "E C S"),
        ("ECR", "E C R"),
        ("S3", "S three"),
        ("API", "A P I"),
        ("SDK", "S D K"),
        ("CLI", "C L I"),
        ("SSH", "S S H"),
        ("SQL", "S Q L"),
        ("RLS", "R L S"),
        ("JWT", "J W T"),
        ("JSON", "J son"),
        ("YAML", "yam ul"),
        ("OAuth", "oh auth"),
        ("UUID", "U U I D"),
        ("URL", "U R L"),
        ("DNS", "D N S"),
        ("ADB", "A D B"),
        ("IPA", "I P A"),
        ("APNs", "A P N's"),
        ("iOS", "eye O S"),
        ("npm", "N P M"),
        ("EAS", "E A S"),
        ("GPT", "G P T"),
        ("TTS", "T T S"),
        ("STT", "S T T"),
        ("CI", "C I"),
    ];

    PRONUNCIATIONS
        .iter()
        .fold(text.to_owned(), |spoken, &(term, pronunciation)| {
            replace_ascii_term(&spoken, term, pronunciation)
        })
}

#[cfg(target_os = "macos")]
fn replace_ascii_term(text: &str, term: &str, replacement: &str) -> String {
    let bytes = text.as_bytes();
    let term_bytes = term.as_bytes();
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    let mut copy_from = 0;

    while cursor + term_bytes.len() <= bytes.len() {
        let matches = bytes[cursor..cursor + term_bytes.len()].eq_ignore_ascii_case(term_bytes);
        let starts_at_boundary = cursor == 0 || !is_technical_word_byte(bytes[cursor - 1]);
        let end = cursor + term_bytes.len();
        let ends_at_boundary = end == bytes.len() || !is_technical_word_byte(bytes[end]);
        if matches && starts_at_boundary && ends_at_boundary {
            output.push_str(&text[copy_from..cursor]);
            output.push_str(replacement);
            cursor = end;
            copy_from = end;
        } else {
            cursor += text[cursor..].chars().next().map_or(1, char::len_utf8);
        }
    }
    output.push_str(&text[copy_from..]);
    output
}

#[cfg(target_os = "macos")]
const fn is_technical_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(target_os = "macos")]
#[allow(
    clippy::cast_precision_loss,
    reason = "audio callback buffers are far below f32's exact integer range"
)]
fn normalized_audio_level(samples: &[f32]) -> f32 {
    const METER_FLOOR_DBFS: f32 = -60.0;
    const METER_CEILING_DBFS: f32 = -3.0;

    if samples.is_empty() {
        return 0.0;
    }
    let rms = (samples
        .iter()
        .map(|sample| {
            if sample.is_finite() {
                sample * sample
            } else {
                0.0
            }
        })
        .sum::<f32>()
        / samples.len() as f32)
        .sqrt();
    if rms <= 0.001 {
        return 0.0;
    }

    let dbfs = 20.0 * rms.log10();
    ((dbfs - METER_FLOOR_DBFS) / (METER_CEILING_DBFS - METER_FLOOR_DBFS)).clamp(0.0, 1.0)
}

#[cfg(target_os = "macos")]
fn microphone_reconnect_delay(attempt: u8) -> Duration {
    let shift = u32::from(attempt.saturating_sub(1).min(4));
    Duration::from_millis(100_u64 << shift)
}

#[cfg(target_os = "macos")]
fn create_microphone_stream(
    audio_tx: mpsc::SyncSender<Vec<f32>>,
    errors: mpsc::SyncSender<String>,
) -> Result<(cpal::Stream, u32), String> {
    use cpal::SampleFormat;
    use cpal::traits::{DeviceTrait, HostTrait};

    let device = cpal::default_host()
        .default_input_device()
        .ok_or_else(|| "No microphone input device is available".to_owned())?;
    let supported = device
        .default_input_config()
        .map_err(|error| format!("Could not read the microphone format: {error}"))?;
    let sample_rate = supported.sample_rate();
    let sample_format = supported.sample_format();
    let config = supported.config();

    let stream = match sample_format {
        SampleFormat::F32 => build_input_stream::<f32>(&device, &config, audio_tx, errors),
        SampleFormat::I16 => build_input_stream::<i16>(&device, &config, audio_tx, errors),
        SampleFormat::I32 => build_input_stream::<i32>(&device, &config, audio_tx, errors),
        format => Err(format!("Unsupported microphone sample format: {format}")),
    }?;
    Ok((stream, sample_rate))
}

#[cfg(target_os = "macos")]
fn start_microphone_stream(
    audio_tx: mpsc::SyncSender<Vec<f32>>,
    errors: mpsc::SyncSender<String>,
) -> Result<(cpal::Stream, u32), String> {
    use cpal::traits::StreamTrait;

    let (stream, sample_rate) = create_microphone_stream(audio_tx, errors)?;
    stream
        .play()
        .map_err(|error| format!("Could not start microphone capture: {error}"))?;
    Ok((stream, sample_rate))
}

#[cfg(target_os = "macos")]
#[allow(clippy::cast_precision_loss)]
fn build_input_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    audio_tx: mpsc::SyncSender<Vec<f32>>,
    errors: mpsc::SyncSender<String>,
) -> Result<cpal::Stream, String>
where
    T: cpal::Sample + cpal::SizedSample + Copy + Send + 'static,
    f32: cpal::FromSample<T>,
{
    use cpal::Sample;
    use cpal::traits::DeviceTrait;

    let channels = usize::from(config.channels.max(1));
    device
        .build_input_stream(
            *config,
            move |input: &[T], _| {
                let mut mono = Vec::with_capacity(input.len() / channels);
                for frame in input.chunks(channels) {
                    let sum = frame.iter().copied().map(f32::from_sample).sum::<f32>();
                    mono.push(sum / frame.len() as f32);
                }
                let _ = audio_tx.try_send(mono);
            },
            move |error| {
                let _ = errors.try_send(format!("Microphone stream changed: {error}"));
            },
            None,
        )
        .map_err(|error| format!("Could not open the microphone: {error}"))
}

#[cfg(target_os = "macos")]
fn transcribe(
    state: &mut whisper_rs::WhisperState,
    audio: &[f32],
    language: &str,
    wake_phrase: &str,
) -> Result<String, String> {
    let transcript = decode_transcript(state, audio, language)?;
    if transcript.no_speech_probability >= WHISPER_NO_SPEECH_REJECT
        || !transcript_is_confident(&transcript, wake_phrase)
    {
        Ok(String::new())
    } else {
        Ok(transcript.text)
    }
}

#[cfg(target_os = "macos")]
struct DecodedTranscript {
    text: String,
    no_speech_probability: f32,
    lexical_tokens: usize,
    minimum_lexical_probability: f32,
}

#[cfg(target_os = "macos")]
fn decode_transcript(
    state: &mut whisper_rs::WhisperState,
    audio: &[f32],
    language: &str,
) -> Result<DecodedTranscript, String> {
    use whisper_rs::{FullParams, SamplingStrategy};

    let mut params = FullParams::new(SamplingStrategy::Greedy { best_of: 1 });
    let threads = std::thread::available_parallelism()
        .map_or(8, std::num::NonZero::get)
        .saturating_sub(2)
        .clamp(4, 12);
    params.set_n_threads(i32::try_from(threads).unwrap_or(8));
    params.set_language(Some(language));
    params.set_translate(false);
    params.set_no_context(true);
    params.set_no_timestamps(true);
    params.set_single_segment(true);
    params.set_max_tokens(192);
    params.set_suppress_blank(true);
    params.set_suppress_nst(true);
    params.set_temperature(0.0);
    params.set_print_special(false);
    params.set_print_progress(false);
    params.set_print_realtime(false);
    params.set_print_timestamps(false);
    state
        .full(params, audio)
        .map_err(|error| format!("Local Whisper transcription failed: {error}"))?;

    let segments = state.as_iter().collect::<Vec<_>>();
    let no_speech_probability = segments
        .iter()
        .map(whisper_rs::WhisperSegment::no_speech_probability)
        .fold(0.0_f32, f32::max);
    let mut lexical_tokens = 0_usize;
    let mut minimum_lexical_probability = 1.0_f32;
    for segment in &segments {
        for token_index in 0..segment.n_tokens() {
            let Some(token) = segment.get_token(token_index) else {
                continue;
            };
            let Ok(token_text) = token.to_str_lossy() else {
                continue;
            };
            if !token_text.starts_with("[_") && token_text.chars().any(char::is_alphanumeric) {
                lexical_tokens += 1;
                minimum_lexical_probability =
                    minimum_lexical_probability.min(token.token_probability());
            }
        }
    }
    let text = segments
        .into_iter()
        .map(|segment| segment.to_string())
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    Ok(DecodedTranscript {
        text,
        no_speech_probability,
        lexical_tokens,
        minimum_lexical_probability: if lexical_tokens == 0 {
            0.0
        } else {
            minimum_lexical_probability
        },
    })
}

#[cfg(target_os = "macos")]
fn transcript_is_confident(transcript: &DecodedTranscript, wake_phrase: &str) -> bool {
    if transcript.lexical_tokens == 0 || !transcript.text.chars().any(char::is_alphanumeric) {
        return false;
    }
    let word_count = transcript
        .text
        .split_whitespace()
        .filter(|word| word.chars().any(char::is_alphanumeric))
        .count();
    if word_count > WHISPER_SHORT_TRANSCRIPT_MAX_WORDS
        || transcript.minimum_lexical_probability >= WHISPER_MIN_SHORT_TOKEN_PROBABILITY
    {
        return true;
    }
    transcript.minimum_lexical_probability >= WHISPER_MIN_WAKE_TOKEN_PROBABILITY
        && !matches!(
            parse_voice_intent(&transcript.text, wake_phrase, false),
            VoiceIntent::NotAddressed
        )
}

#[cfg(target_os = "macos")]
fn whisper_language(locale: &str) -> String {
    locale
        .trim()
        .split(['-', '_'])
        .next()
        .filter(|language| !language.is_empty())
        .unwrap_or("en")
        .to_lowercase()
}

#[cfg(target_os = "macos")]
struct StreamingResampler {
    ratio: f64,
    input: Vec<f32>,
    position: f64,
}

#[cfg(target_os = "macos")]
impl StreamingResampler {
    fn new(input_rate: u32) -> Self {
        Self {
            ratio: f64::from(input_rate) / f64::from(WHISPER_SAMPLE_RATE),
            input: Vec::new(),
            position: 0.0,
        }
    }

    fn reset(&mut self) {
        self.input.clear();
        self.position = 0.0;
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        reason = "audio buffers are small and positions are always finite and non-negative"
    )]
    fn push(&mut self, samples: &[f32]) -> Vec<f32> {
        self.input.extend_from_slice(samples);
        let mut output =
            Vec::with_capacity((samples.len() as f64 / self.ratio).ceil() as usize + 1);
        while self.position + 1.0 < self.input.len() as f64 {
            let left = self.position.floor() as usize;
            let fraction = (self.position - left as f64) as f32;
            output.push(self.input[left] + (self.input[left + 1] - self.input[left]) * fraction);
            self.position += self.ratio;
        }
        let consumed = self.position.floor() as usize;
        if consumed > 0 {
            self.input.drain(..consumed.min(self.input.len()));
            self.position -= consumed as f64;
        }
        output
    }
}

#[cfg(target_os = "macos")]
struct UtteranceDetector {
    neural_vad: Option<SileroVad>,
    neural_speech_probability: Option<f32>,
    speech_confirmation_mode: SpeechConfirmationMode,
    pre_roll: VecDeque<f32>,
    utterance: Vec<f32>,
    silence_limit: usize,
    silent_samples: usize,
    noise_rms: f32,
    noise_band_rms: f32,
    playback_noise_rms: f32,
    playback_noise_band_rms: f32,
    playback_peak_rms: f32,
    playback_peak_band_rms: f32,
    playback_warmup_samples: usize,
    playback_probe_samples: usize,
    playback_probe_request: PlaybackProbeRequest,
    playback_probe_echo_tail_samples: usize,
    playback_was_active: bool,
    observed_samples: usize,
    voice_isolation: bool,
    candidate_samples: usize,
    candidate_voiced_samples: usize,
    candidate_articulation_gap: usize,
    candidate_min_band_rms: f32,
    candidate_max_band_rms: f32,
    candidate_min_spectral_flatness: f32,
    high_pass_input: f32,
    high_pass_output: f32,
    speech_low_pass: f32,
    previous_band_sample: f32,
    analysis_frame: Vec<f32>,
    #[cfg(test)]
    last_metrics: VadMetrics,
    speaking: bool,
}

#[cfg(target_os = "macos")]
impl UtteranceDetector {
    fn new(silence_ms: u64) -> Self {
        Self {
            neural_vad: if cfg!(test) {
                None
            } else {
                SileroVad::new().ok()
            },
            neural_speech_probability: None,
            speech_confirmation_mode: SpeechConfirmationMode::Normal,
            pre_roll: VecDeque::with_capacity(PRE_ROLL_SAMPLES),
            utterance: Vec::new(),
            silence_limit: (silence_ms.clamp(500, 3_000) as usize * WHISPER_SAMPLE_RATE as usize)
                / 1_000,
            silent_samples: 0,
            noise_rms: 0.002,
            noise_band_rms: 0.001,
            playback_noise_rms: 0.002,
            playback_noise_band_rms: 0.001,
            playback_peak_rms: 0.002,
            playback_peak_band_rms: 0.001,
            playback_warmup_samples: 0,
            playback_probe_samples: 0,
            playback_probe_request: PlaybackProbeRequest::Idle,
            playback_probe_echo_tail_samples: 0,
            playback_was_active: false,
            observed_samples: 0,
            voice_isolation: false,
            candidate_samples: 0,
            candidate_voiced_samples: 0,
            candidate_articulation_gap: 0,
            candidate_min_band_rms: f32::MAX,
            candidate_max_band_rms: 0.0,
            candidate_min_spectral_flatness: f32::MAX,
            high_pass_input: 0.0,
            high_pass_output: 0.0,
            speech_low_pass: 0.0,
            previous_band_sample: 0.0,
            analysis_frame: Vec::with_capacity(1_024),
            #[cfg(test)]
            last_metrics: VadMetrics::default(),
            speaking: false,
        }
    }

    fn reset(&mut self) {
        if let Some(neural_vad) = self.neural_vad.as_mut() {
            neural_vad.reset();
        }
        self.neural_speech_probability = None;
        self.speech_confirmation_mode = SpeechConfirmationMode::Normal;
        self.pre_roll.clear();
        self.utterance.clear();
        self.silent_samples = 0;
        self.reset_candidate();
        self.reset_playback_probe_candidate();
        self.playback_probe_echo_tail_samples = 0;
        self.high_pass_input = 0.0;
        self.high_pass_output = 0.0;
        self.speech_low_pass = 0.0;
        self.previous_band_sample = 0.0;
        self.speaking = false;
    }

    fn rearm(&mut self, silence_ms: u64) {
        self.silence_limit =
            (silence_ms.clamp(500, 3_000) as usize * WHISPER_SAMPLE_RATE as usize) / 1_000;
        self.reset();
    }

    fn set_voice_isolation(&mut self, enabled: bool) {
        if self.voice_isolation != enabled {
            self.voice_isolation = enabled;
            self.observed_samples = 0;
            self.reset();
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        clippy::too_many_lines,
        reason = "the callback owns one atomic VAD transition; its buffers are small"
    )]
    fn push(&mut self, samples: &[f32], playback_active: bool) -> Option<Vec<f32>> {
        if samples.is_empty() {
            return None;
        }
        self.update_neural_speech_probability(samples);
        let metrics = self.measure(samples);
        #[cfg(test)]
        {
            self.last_metrics = metrics;
        }

        if !self.speaking {
            self.push_pre_roll(samples);
            if self.playback_probe_echo_tail_samples > 0 {
                self.playback_probe_echo_tail_samples = self
                    .playback_probe_echo_tail_samples
                    .saturating_sub(samples.len());
                return None;
            }
            if playback_active != self.playback_was_active {
                self.playback_was_active = playback_active;
                self.reset_candidate();
                self.reset_playback_probe_candidate();
                if playback_active {
                    self.playback_peak_rms = self.noise_rms;
                    self.playback_peak_band_rms = self.noise_band_rms;
                    self.playback_warmup_samples = PLAYBACK_WARMUP_SAMPLES;
                }
            }

            if playback_active {
                if self.playback_warmup_samples > 0 {
                    // This is known Roadie output, so converge quickly before looking for
                    // double-talk. A slow baseline makes the first syllables look like a user.
                    self.update_playback_noise(metrics, 0.55);
                    self.playback_warmup_samples =
                        self.playback_warmup_samples.saturating_sub(samples.len());
                    return None;
                }

                if self.has_playback_probe_evidence(metrics) {
                    self.playback_probe_samples =
                        self.playback_probe_samples.saturating_add(samples.len());
                    if self.playback_probe_samples >= PLAYBACK_PROBE_CONFIRM_SAMPLES {
                        self.playback_probe_request = PlaybackProbeRequest::Requested;
                        self.playback_probe_samples = 0;
                        return None;
                    }
                } else {
                    self.playback_probe_samples = 0;
                }
            } else {
                self.observed_samples = self.observed_samples.saturating_add(samples.len());
                if self.observed_samples <= VAD_CALIBRATION_SAMPLES {
                    self.update_noise(metrics, 0.12);
                    return None;
                }
            }

            let thresholds = self.start_thresholds(playback_active);
            let acoustic_speech_evidence = has_speech_evidence(
                metrics,
                thresholds.raw,
                thresholds.band,
                thresholds.strong_band,
                self.voice_isolation && !playback_active,
            );
            let neural_threshold =
                if self.speech_confirmation_mode == SpeechConfirmationMode::AfterPlaybackPause {
                    if self.voice_isolation && !playback_active {
                        0.20
                    } else {
                        0.30
                    }
                } else if self.voice_isolation && !playback_active {
                    0.055
                } else {
                    0.10
                };
            let neural_speech_evidence = self
                .neural_speech_probability
                .is_none_or(|probability| probability >= neural_threshold);
            let speech_evidence = acoustic_speech_evidence && neural_speech_evidence;

            if speech_evidence {
                self.candidate_samples = self.candidate_samples.saturating_add(samples.len());
                let isolated = self.voice_isolation && !playback_active;
                let spectral_detail_ratio = if isolated {
                    0.035
                } else {
                    VAD_MIN_SPECTRAL_DETAIL_RATIO
                };
                let min_zero_crossing_rate = if isolated {
                    0.002
                } else {
                    VAD_MIN_SPEECH_ZERO_CROSSING_RATE
                };
                let min_voice_periodicity = if isolated {
                    0.32
                } else {
                    VAD_MIN_VOICE_PERIODICITY
                };
                let has_spectral_detail =
                    metrics.treble_rms >= metrics.speech_band_rms * spectral_detail_ratio;
                let has_vocal_articulation = metrics.zero_crossing_rate >= min_zero_crossing_rate
                    || metrics.voice_periodicity >= min_voice_periodicity;
                if has_spectral_detail && has_vocal_articulation {
                    self.candidate_voiced_samples =
                        self.candidate_voiced_samples.saturating_add(samples.len());
                    self.candidate_articulation_gap = 0;
                } else {
                    self.candidate_articulation_gap = self
                        .candidate_articulation_gap
                        .saturating_add(samples.len());
                }
                self.candidate_min_band_rms = self.candidate_min_band_rms.min(metrics.band_rms);
                self.candidate_max_band_rms = self.candidate_max_band_rms.max(metrics.band_rms);
                self.candidate_min_spectral_flatness = self
                    .candidate_min_spectral_flatness
                    .min(metrics.spectral_flatness);

                if self.candidate_samples >= thresholds.confirm_samples {
                    let isolated = self.voice_isolation && !playback_active;
                    let modulation_ratio = if playback_active {
                        1.28
                    } else if isolated {
                        1.025
                    } else {
                        1.12
                    };
                    let speech_is_modulated = self.candidate_max_band_rms
                        >= self.candidate_min_band_rms.max(0.000_1) * modulation_ratio;
                    let minimum_voiced_samples = if playback_active {
                        thresholds.confirm_samples / 2
                    } else if isolated {
                        thresholds.confirm_samples / 8
                    } else {
                        thresholds.confirm_samples / 4
                    };
                    let maximum_articulation_gap = if isolated {
                        thresholds.confirm_samples / 2
                    } else {
                        thresholds.confirm_samples / 5
                    };
                    let maximum_spectral_flatness = if isolated {
                        0.42
                    } else {
                        VAD_MAX_SPEECH_SPECTRAL_FLATNESS
                    };
                    let voice_is_articulated = self.candidate_voiced_samples
                        >= minimum_voiced_samples
                        && self.candidate_articulation_gap <= maximum_articulation_gap
                        && self.candidate_min_spectral_flatness <= maximum_spectral_flatness;
                    if speech_is_modulated && voice_is_articulated {
                        let confirmed_samples = self.candidate_samples;
                        if playback_active {
                            self.trim_playback_pre_roll(confirmed_samples);
                        }
                        self.speaking = true;
                        self.utterance.extend(self.pre_roll.drain(..));
                        self.reset_candidate();
                    } else {
                        if playback_active {
                            self.update_playback_noise(metrics, 0.35);
                        } else {
                            self.update_noise(metrics, 0.35);
                        }
                        self.reset_candidate();
                    }
                }
            } else {
                if playback_active {
                    self.update_playback_noise(metrics, 0.035);
                } else {
                    self.update_noise(metrics, 0.015);
                }
                self.reset_candidate();
            }
            return None;
        }

        self.utterance.extend_from_slice(samples);
        let (continue_raw_threshold, continue_band_threshold) = if self.voice_isolation {
            (
                (self.noise_rms * 1.08 + 0.000_4).clamp(0.001_5, 0.20),
                (self.noise_band_rms * 1.10 + 0.000_2).clamp(0.000_8, 0.15),
            )
        } else {
            (
                (self.noise_rms * 1.22 + 0.001).clamp(0.004, 0.20),
                (self.noise_band_rms * 1.28 + 0.000_5).clamp(0.002, 0.15),
            )
        };
        let acoustic_speech_active = metrics.raw_rms >= continue_raw_threshold
            || metrics.band_rms >= continue_band_threshold;
        let speech_active = neural_endpoint_speech_active(
            self.neural_speech_probability,
            metrics,
            acoustic_speech_active,
            self.voice_isolation,
        );
        if speech_active {
            self.silent_samples = 0;
        } else {
            self.silent_samples += samples.len();
        }
        let silence_complete = self.silent_samples >= self.silence_limit
            && self.utterance.len() >= MIN_UTTERANCE_SAMPLES;
        let playback_confirmation_ready =
            playback_active && self.utterance.len() >= PLAYBACK_CONFIRMATION_SAMPLES;
        if silence_complete
            || playback_confirmation_ready
            || self.utterance.len() >= MAX_UTTERANCE_SAMPLES
        {
            self.speaking = false;
            self.silent_samples = 0;
            self.pre_roll.clear();
            return Some(std::mem::take(&mut self.utterance));
        }
        None
    }

    fn update_neural_speech_probability(&mut self, samples: &[f32]) {
        let Some(neural_vad) = self.neural_vad.as_mut() else {
            return;
        };
        if let Ok(probabilities) = neural_vad.push(samples) {
            if let Some(probability) = probabilities.last() {
                self.neural_speech_probability = Some(*probability);
            }
        } else {
            // The deterministic spectral detector remains available if the embedded model
            // cannot process an unexpected audio frame.
            self.neural_vad = None;
            self.neural_speech_probability = None;
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "CoreAudio callback buffers are far below f32's exact integer range"
    )]
    fn measure(&mut self, samples: &[f32]) -> VadMetrics {
        let mut raw_energy = 0.0_f32;
        let mut band_energy = 0.0_f32;
        let mut speech_band_energy = 0.0_f32;
        let mut treble_energy = 0.0_f32;
        let mut zero_crossings = 0_usize;
        self.analysis_frame.clear();
        for &sample in samples {
            raw_energy += sample * sample;
            let filtered = sample - self.high_pass_input + 0.92 * self.high_pass_output;
            self.high_pass_input = sample;
            self.high_pass_output = filtered;
            band_energy += filtered * filtered;
            self.speech_low_pass += VAD_SPEECH_LOW_PASS_ALPHA * (filtered - self.speech_low_pass);
            let treble = filtered - self.speech_low_pass;
            self.analysis_frame.push(self.speech_low_pass);
            speech_band_energy += self.speech_low_pass * self.speech_low_pass;
            treble_energy += treble * treble;
            if filtered.abs() >= 0.002
                && self.previous_band_sample.abs() >= 0.002
                && filtered.is_sign_positive() != self.previous_band_sample.is_sign_positive()
            {
                zero_crossings += 1;
            }
            self.previous_band_sample = filtered;
        }
        let count = samples.len() as f32;
        VadMetrics {
            raw_rms: (raw_energy / count).sqrt(),
            band_rms: (band_energy / count).sqrt(),
            speech_band_rms: (speech_band_energy / count).sqrt(),
            treble_rms: (treble_energy / count).sqrt(),
            zero_crossing_rate: zero_crossings as f32 / count,
            voice_periodicity: voice_periodicity(&self.analysis_frame),
            spectral_flatness: spectral_flatness(samples),
        }
    }

    fn update_noise(&mut self, metrics: VadMetrics, weight: f32) {
        self.noise_rms = self.noise_rms * (1.0 - weight) + metrics.raw_rms * weight;
        self.noise_band_rms = self.noise_band_rms * (1.0 - weight) + metrics.band_rms * weight;
    }

    fn start_thresholds(&self, playback_active: bool) -> VadStartThresholds {
        let isolated = self.voice_isolation && !playback_active;
        let (raw_floor, band_floor, raw_multiplier, band_multiplier, confirm_samples) =
            if playback_active {
                (
                    self.noise_rms.max(self.playback_noise_rms),
                    self.noise_band_rms.max(self.playback_noise_band_rms),
                    1.25,
                    1.30,
                    WHISPER_SAMPLE_RATE as usize * 34 / 100,
                )
            } else if isolated {
                (
                    self.noise_rms,
                    self.noise_band_rms,
                    1.12,
                    1.18,
                    WHISPER_SAMPLE_RATE as usize / 10,
                )
            } else {
                (
                    self.noise_rms,
                    self.noise_band_rms,
                    1.35,
                    1.45,
                    VAD_START_CONFIRM_SAMPLES,
                )
            };
        if isolated {
            VadStartThresholds {
                raw: (raw_floor * raw_multiplier + 0.000_5).clamp(0.002, 0.20),
                band: (band_floor * band_multiplier + 0.000_25).clamp(0.000_9, 0.15),
                strong_band: band_floor * 1.65 + 0.000_5,
                confirm_samples,
            }
        } else {
            let raw_floor = if playback_active {
                raw_floor.max(self.playback_peak_rms)
            } else {
                raw_floor
            };
            let band_floor = if playback_active {
                band_floor.max(self.playback_peak_band_rms)
            } else {
                band_floor
            };
            VadStartThresholds {
                raw: (raw_floor * raw_multiplier + 0.001_5).clamp(0.006, 0.25),
                band: (band_floor * band_multiplier + 0.000_8).clamp(0.002_5, 0.20),
                strong_band: band_floor * if playback_active { 1.8 } else { 2.35 } + 0.001_5,
                confirm_samples,
            }
        }
    }

    fn update_playback_noise(&mut self, metrics: VadMetrics, weight: f32) {
        self.playback_noise_rms =
            self.playback_noise_rms * (1.0 - weight) + metrics.raw_rms * weight;
        self.playback_noise_band_rms =
            self.playback_noise_band_rms * (1.0 - weight) + metrics.band_rms * weight;
        self.playback_peak_rms = (self.playback_peak_rms * 0.997).max(metrics.raw_rms);
        self.playback_peak_band_rms = (self.playback_peak_band_rms * 0.997).max(metrics.band_rms);
    }

    fn has_playback_probe_evidence(&self, metrics: VadMetrics) -> bool {
        let raw_floor = self.noise_rms.max(self.playback_noise_rms).max(0.001);
        let band_floor = self
            .noise_band_rms
            .max(self.playback_noise_band_rms)
            .max(0.000_5);
        let raw_rise = metrics.raw_rms >= raw_floor * 1.10 + 0.000_5;
        let band_rise = metrics.band_rms >= band_floor * 1.10 + 0.000_4;
        let has_spectral_detail = metrics.treble_rms >= metrics.speech_band_rms * 0.055;
        let has_vocal_articulation =
            metrics.zero_crossing_rate >= 0.004 || metrics.voice_periodicity >= 0.42;
        let voice_shaped = metrics.spectral_flatness <= 0.38;
        raw_rise && band_rise && has_spectral_detail && has_vocal_articulation && voice_shaped
    }

    fn take_playback_probe_request(&mut self) -> bool {
        std::mem::take(&mut self.playback_probe_request) == PlaybackProbeRequest::Requested
    }

    fn begin_playback_probe_confirmation(&mut self) {
        self.reset();
        self.speech_confirmation_mode = SpeechConfirmationMode::AfterPlaybackPause;
        self.playback_was_active = false;
        self.observed_samples = VAD_CALIBRATION_SAMPLES.saturating_add(1);
        self.playback_probe_echo_tail_samples = PLAYBACK_PROBE_ECHO_TAIL_SAMPLES;
    }

    fn cancel_playback_probe(&mut self) {
        self.reset();
        self.playback_was_active = false;
        self.observed_samples = VAD_CALIBRATION_SAMPLES.saturating_add(1);
    }

    fn reset_candidate(&mut self) {
        self.candidate_samples = 0;
        self.candidate_voiced_samples = 0;
        self.candidate_articulation_gap = 0;
        self.candidate_min_band_rms = f32::MAX;
        self.candidate_max_band_rms = 0.0;
        self.candidate_min_spectral_flatness = f32::MAX;
    }

    fn reset_playback_probe_candidate(&mut self) {
        self.playback_probe_samples = 0;
        self.playback_probe_request = PlaybackProbeRequest::Idle;
    }

    fn push_pre_roll(&mut self, samples: &[f32]) {
        self.pre_roll.extend(samples.iter().copied());
        while self.pre_roll.len() > PRE_ROLL_SAMPLES {
            self.pre_roll.pop_front();
        }
    }

    fn trim_playback_pre_roll(&mut self, confirmed_samples: usize) {
        let retained = confirmed_samples.saturating_add(PLAYBACK_PRE_ROLL_GUARD_SAMPLES);
        while self.pre_roll.len() > retained {
            self.pre_roll.pop_front();
        }
    }
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Default)]
struct VadMetrics {
    raw_rms: f32,
    band_rms: f32,
    speech_band_rms: f32,
    treble_rms: f32,
    zero_crossing_rate: f32,
    voice_periodicity: f32,
    spectral_flatness: f32,
}

#[cfg(target_os = "macos")]
#[allow(
    clippy::cast_precision_loss,
    reason = "short audio frames and fixed filterbank indexes fit f32 precision"
)]
fn spectral_flatness(samples: &[f32]) -> f32 {
    const FREQUENCIES: [f32; 15] = [
        180.0, 240.0, 320.0, 420.0, 550.0, 720.0, 940.0, 1_220.0, 1_580.0, 2_050.0, 2_660.0,
        3_450.0, 4_200.0, 5_200.0, 6_200.0,
    ];
    if samples.is_empty() {
        return 1.0;
    }

    let mut log_sum = 0.0_f32;
    let mut arithmetic_sum = 0.0_f32;
    for frequency in FREQUENCIES {
        let coefficient =
            2.0 * (2.0 * std::f32::consts::PI * frequency / WHISPER_SAMPLE_RATE as f32).cos();
        let mut previous = 0.0_f32;
        let mut previous_two = 0.0_f32;
        for &sample in samples {
            let current = sample + coefficient * previous - previous_two;
            previous_two = previous;
            previous = current;
        }
        let power = (previous * previous + previous_two * previous_two
            - coefficient * previous * previous_two)
            .max(1.0e-12);
        log_sum += power.ln();
        arithmetic_sum += power;
    }
    let count = FREQUENCIES.len() as f32;
    let geometric_mean = (log_sum / count).exp();
    (geometric_mean / (arithmetic_sum / count).max(1.0e-12)).clamp(0.0, 1.0)
}

#[cfg(target_os = "macos")]
#[allow(
    clippy::cast_precision_loss,
    reason = "CoreAudio callback frames are far below f32's exact integer range"
)]
fn voice_periodicity(samples: &[f32]) -> f32 {
    const MIN_LAG: usize = 46;
    const MAX_LAG: usize = 152;
    if samples.len() <= MAX_LAG + 32 {
        return 0.0;
    }

    let mean = samples.iter().sum::<f32>() / samples.len() as f32;
    let mut strongest = 0.0_f32;
    for lag in (MIN_LAG..=MAX_LAG).step_by(2) {
        let mut correlation = 0.0_f32;
        let mut leading_energy = 0.0_f32;
        let mut trailing_energy = 0.0_f32;
        for index in lag..samples.len() {
            let leading = samples[index] - mean;
            let trailing = samples[index - lag] - mean;
            correlation += leading * trailing;
            leading_energy += leading * leading;
            trailing_energy += trailing * trailing;
        }
        let normalization = (leading_energy * trailing_energy).sqrt();
        if normalization > 1.0e-9 {
            strongest = strongest.max(correlation / normalization);
        }
    }
    strongest.clamp(0.0, 1.0)
}

#[cfg(target_os = "macos")]
struct VadStartThresholds {
    raw: f32,
    band: f32,
    strong_band: f32,
    confirm_samples: usize,
}

#[cfg(target_os = "macos")]
fn has_speech_evidence(
    metrics: VadMetrics,
    raw_threshold: f32,
    band_threshold: f32,
    strong_band_threshold: f32,
    voice_isolation: bool,
) -> bool {
    let birdlike_treble = !voice_isolation
        && metrics.zero_crossing_rate >= 0.16
        && metrics.treble_rms >= metrics.speech_band_rms * 1.18
        && metrics.treble_rms >= band_threshold;
    let voice_shaped_band = metrics.speech_band_rms >= band_threshold
        && metrics.speech_band_rms
            >= metrics.treble_rms * if voice_isolation { 1.08 } else { 1.45 };
    !birdlike_treble
        && metrics.band_rms >= band_threshold
        && (metrics.raw_rms >= raw_threshold
            || metrics.band_rms >= strong_band_threshold
            || voice_shaped_band)
}

#[cfg(target_os = "macos")]
fn neural_endpoint_speech_active(
    probability: Option<f32>,
    metrics: VadMetrics,
    acoustic_speech_active: bool,
    voice_isolation: bool,
) -> bool {
    let Some(probability) = probability else {
        return acoustic_speech_active;
    };
    let threshold = if voice_isolation { 0.12 } else { 0.24 };
    let articulated = metrics.voice_periodicity >= if voice_isolation { 0.28 } else { 0.42 }
        || metrics.zero_crossing_rate >= if voice_isolation { 0.002 } else { 0.006 };
    probability >= threshold
        || (probability >= threshold * 0.55 && acoustic_speech_active && articulated)
}

#[cfg(not(target_os = "macos"))]
fn run(
    receiver: mpsc::Receiver<SpeechCommand>,
    events: tokio_mpsc::UnboundedSender<SpeechEngineEvent>,
    _model_path: PathBuf,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            SpeechCommand::StartListening { .. } | SpeechCommand::Speak { .. } => {
                let _ = events.send(SpeechEngineEvent::Error(
                    "Local microphone transcription is only available in the macOS app".into(),
                ));
            }
            SpeechCommand::StopListening => {
                let _ = events.send(SpeechEngineEvent::ListeningStopped);
            }
            SpeechCommand::StopSpeaking => {
                let _ = events.send(SpeechEngineEvent::SpeakingCancelled);
            }
            SpeechCommand::PauseSpeaking => {
                let _ = events.send(SpeechEngineEvent::SpeakingPaused);
            }
            SpeechCommand::Shutdown => break,
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn technical_terms_are_pronounced_without_changing_surrounding_text() {
        assert_eq!(
            normalize_technical_pronunciations(
                "Deploy the Supabase API to AWS ECS, then run pgTAP over SSH."
            ),
            "Deploy the Supa base A P I to A W S E C S, then run P G tap over S S H."
        );
    }

    #[test]
    fn pronunciation_aliases_respect_word_boundaries_and_unicode() {
        assert_eq!(
            normalize_technical_pronunciations(
                "The className and CIPHER stay exact; AWS’s URL works."
            ),
            "The className and CIPHER stay exact; A W S’s U R L works."
        );
    }

    #[test]
    fn locale_maps_to_whisper_language() {
        assert_eq!(whisper_language("en-US"), "en");
        assert_eq!(whisper_language("es_MX"), "es");
        assert_eq!(whisper_language("  "), "en");
    }

    #[test]
    fn live_input_meter_uses_dbfs_instead_of_saturating_at_normal_voice_levels() {
        assert!(normalized_audio_level(&[0.0; 320]).abs() < f32::EPSILON);

        let normal_voice = normalized_audio_level(&[0.031_622_78; 320]);
        assert!((0.50..=0.55).contains(&normal_voice));

        assert!((normalized_audio_level(&[1.0; 320]) - 1.0).abs() < f32::EPSILON);
        assert!(normalized_audio_level(&[f32::NAN; 320]).abs() < f32::EPSILON);
    }

    #[test]
    fn microphone_reconnect_uses_short_bounded_backoff() {
        assert_eq!(microphone_reconnect_delay(1), Duration::from_millis(100));
        assert_eq!(microphone_reconnect_delay(2), Duration::from_millis(200));
        assert_eq!(microphone_reconnect_delay(5), Duration::from_millis(1_600));
        assert_eq!(microphone_reconnect_delay(20), Duration::from_millis(1_600));
    }

    #[test]
    fn whisper_rejects_low_confidence_short_noise_hallucinations() {
        let punctuation = DecodedTranscript {
            text: ".".into(),
            no_speech_probability: 0.0,
            lexical_tokens: 0,
            minimum_lexical_probability: 0.0,
        };
        let hallucination = DecodedTranscript {
            text: "Thank you.".into(),
            no_speech_probability: 0.0,
            lexical_tokens: 2,
            minimum_lexical_probability: 0.26,
        };
        let clear_command = DecodedTranscript {
            text: "Hey Codex".into(),
            no_speech_probability: 0.0,
            lexical_tokens: 2,
            minimum_lexical_probability: 0.91,
        };
        let soft_wake = DecodedTranscript {
            text: "Hey Kodex".into(),
            no_speech_probability: 0.0,
            lexical_tokens: 2,
            minimum_lexical_probability: 0.07,
        };
        let unrelated_short_phrase = DecodedTranscript {
            text: "Hey there".into(),
            no_speech_probability: 0.0,
            lexical_tokens: 2,
            minimum_lexical_probability: 0.22,
        };
        assert!(!transcript_is_confident(&punctuation, "hey codex"));
        assert!(!transcript_is_confident(&hallucination, "hey codex"));
        assert!(transcript_is_confident(&clear_command, "hey codex"));
        assert!(transcript_is_confident(&soft_wake, "hey codex"));
        assert!(!transcript_is_confident(
            &unrelated_short_phrase,
            "hey codex"
        ));
    }

    #[test]
    fn resampler_converts_48_khz_to_16_khz() {
        let mut resampler = StreamingResampler::new(48_000);
        let input = vec![0.25; 4_800];
        let output = resampler.push(&input);
        assert!((1_599..=1_601).contains(&output.len()));
        assert!(output.iter().all(|sample| (*sample - 0.25).abs() < 0.000_1));
    }

    #[test]
    fn detector_keeps_pre_roll_and_finishes_after_silence() {
        let mut detector = UtteranceDetector::new(500);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
        }
        for frame in 0..14 {
            let amplitude = if frame % 3 == 0 { 0.09 } else { 0.04 };
            assert!(
                detector
                    .push(&tone_frame(frame * 320, 320, 230.0, amplitude), false)
                    .is_none()
            );
        }
        assert!(detector.speaking);
        let utterance = detector
            .push(&vec![0.0; 8_000], false)
            .expect("silence should finish the utterance");
        assert!(utterance.len() >= PRE_ROLL_SAMPLES + 8_000);
    }

    #[test]
    fn neural_endpoint_closes_on_loud_non_speech_instead_of_waiting_for_hard_cap() {
        let loud_cabin_noise = VadMetrics {
            raw_rms: 0.08,
            band_rms: 0.04,
            speech_band_rms: 0.02,
            treble_rms: 0.02,
            zero_crossing_rate: 0.001,
            voice_periodicity: 0.12,
            spectral_flatness: 0.62,
        };
        assert!(!neural_endpoint_speech_active(
            Some(0.03),
            loud_cabin_noise,
            true,
            true,
        ));
        assert!(neural_endpoint_speech_active(
            Some(0.91),
            loud_cabin_noise,
            false,
            true,
        ));
    }

    #[test]
    fn neural_endpoint_keeps_quiet_articulated_speech_open() {
        let quiet_voice = VadMetrics {
            raw_rms: 0.006,
            band_rms: 0.003,
            speech_band_rms: 0.002_5,
            treble_rms: 0.000_8,
            zero_crossing_rate: 0.004,
            voice_periodicity: 0.48,
            spectral_flatness: 0.14,
        };
        assert!(neural_endpoint_speech_active(
            Some(0.19),
            quiet_voice,
            true,
            true,
        ));
    }

    #[test]
    fn voice_isolation_profile_accepts_a_quiet_processed_wake_phrase() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.000_5; 320], false).is_none());
        }
        for frame in 30..45 {
            let start = frame * 320;
            let amplitude = if frame % 3 == 0 { 0.012 } else { 0.007 };
            let mut processed_voice = tone_frame(start, 320, 225.0, amplitude);
            add_tone(&mut processed_voice, start, 1_150.0, amplitude * 0.22);
            assert!(detector.push(&processed_voice, false).is_none());
            if detector.speaking {
                break;
            }
        }
        assert!(detector.speaking);
    }

    #[test]
    fn voice_isolation_profile_still_rejects_a_short_impact() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.000_5; 320], false).is_none());
        }
        for frame in 30..34 {
            let start = frame * 320;
            let decay = 0.08 * 0.55_f32.powi(i32::try_from(frame - 30).unwrap_or(0));
            let mut impact = pseudo_noise_frame(start, 320, decay);
            add_tone(&mut impact, start, 62.0, decay * 0.35);
            assert!(detector.push(&impact, false).is_none());
        }
        for _ in 0..10 {
            assert!(detector.push(&vec![0.000_5; 320], false).is_none());
        }
        assert!(!detector.speaking);
    }

    #[test]
    fn stationary_road_noise_does_not_open_an_utterance() {
        let mut detector = UtteranceDetector::new(500);
        for frame in 0..150 {
            let start = frame * 320;
            let mut noise = tone_frame(start, 320, 75.0, 0.045);
            add_tone(&mut noise, start, 850.0, 0.006);
            assert!(detector.push(&noise, false).is_none());
            assert!(!detector.speaking);
        }
    }

    #[test]
    fn steady_road_level_change_is_learned_instead_of_treated_as_speech() {
        let mut detector = UtteranceDetector::new(500);
        for frame in 0..30 {
            let start = frame * 320;
            let mut noise = tone_frame(start, 320, 80.0, 0.025);
            add_tone(&mut noise, start, 900.0, 0.004);
            assert!(detector.push(&noise, false).is_none());
        }
        for frame in 30..70 {
            let start = frame * 320;
            let mut louder_road = tone_frame(start, 320, 80.0, 0.065);
            add_tone(&mut louder_road, start, 900.0, 0.010);
            assert!(detector.push(&louder_road, false).is_none());
            assert!(!detector.speaking);
        }
    }

    #[test]
    fn suspension_bump_does_not_open_an_utterance() {
        let mut detector = UtteranceDetector::new(500);
        for frame in 0..30 {
            let mut road = tone_frame(frame * 320, 320, 75.0, 0.035);
            add_tone(&mut road, frame * 320, 850.0, 0.005);
            assert!(detector.push(&road, false).is_none());
        }
        for frame in 30..38 {
            let start = frame * 320;
            let decay = 0.32 * 0.68_f32.powi(i32::try_from(frame - 30).unwrap_or(0));
            let mut bump = tone_frame(start, 320, 58.0, 0.035 + decay);
            add_tone(&mut bump, start, 720.0, decay * 0.45);
            add_tone(&mut bump, start, 2_400.0, decay * 0.20);
            assert!(detector.push(&bump, false).is_none());
            assert!(!detector.speaking);
        }
    }

    #[test]
    #[allow(
        clippy::cast_precision_loss,
        reason = "small synthetic frame indexes are exactly representable enough for f32"
    )]
    fn window_motor_swell_does_not_open_an_utterance() {
        let mut detector = UtteranceDetector::new(500);
        for frame in 0..30 {
            let road = pseudo_noise_frame(frame * 320, 320, 0.018);
            assert!(detector.push(&road, false).is_none());
        }
        for frame in 30..180 {
            let progress = (frame - 30) as f32 / 150.0;
            let envelope = (progress * std::f32::consts::PI).sin().max(0.0);
            let mut window = pseudo_noise_frame(frame * 320, 320, 0.025 + envelope * 0.12);
            add_tone(&mut window, frame * 320, 64.0, 0.025 + envelope * 0.08);
            assert!(detector.push(&window, false).is_none());
            assert!(!detector.speaking);
        }
    }

    #[test]
    fn modulated_bird_chirps_do_not_open_an_utterance() {
        const CHIRP_FREQUENCIES: [f32; 9] = [
            2_800.0, 3_110.0, 3_420.0, 3_730.0, 4_040.0, 4_350.0, 4_660.0, 4_970.0, 5_280.0,
        ];
        let mut detector = UtteranceDetector::new(500);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
        }

        for frame in 30..150 {
            let start = frame * 320;
            let frequency = CHIRP_FREQUENCIES[frame % CHIRP_FREQUENCIES.len()];
            let amplitude = if frame % 5 == 0 { 0.15 } else { 0.075 };
            let mut birds = tone_frame(start, 320, frequency, amplitude);
            add_tone(&mut birds, start, frequency + 620.0, amplitude * 0.24);
            assert!(detector.push(&birds, false).is_none());
            assert!(!detector.speaking);
        }
    }

    #[test]
    fn nearby_voice_over_birds_still_opens_an_utterance() {
        let mut detector = UtteranceDetector::new(500);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
        }

        for frame in 30..60 {
            let start = frame * 320;
            let voice_amplitude = if frame % 4 == 0 { 0.15 } else { 0.08 };
            let mut voice_and_birds = tone_frame(start, 320, 235.0, voice_amplitude);
            add_tone(&mut voice_and_birds, start, 1_350.0, voice_amplitude * 0.38);
            add_tone(&mut voice_and_birds, start, 4_200.0, 0.035);
            assert!(detector.push(&voice_and_birds, false).is_none());
            if detector.speaking {
                break;
            }
        }
        assert!(detector.speaking);
    }

    #[test]
    fn bird_chirps_do_not_trigger_barge_in_during_playback() {
        const CHIRP_FREQUENCIES: [f32; 7] = [
            3_100.0, 3_470.0, 3_840.0, 4_210.0, 4_580.0, 4_950.0, 5_320.0,
        ];
        let mut detector = UtteranceDetector::new(500);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
        }
        for frame in 0..30 {
            let mut playback = tone_frame(frame * 320, 320, 220.0, 0.035);
            add_tone(&mut playback, frame * 320, 1_050.0, 0.009);
            assert!(detector.push(&playback, true).is_none());
        }

        for frame in 30..120 {
            let start = frame * 320;
            let frequency = CHIRP_FREQUENCIES[frame % CHIRP_FREQUENCIES.len()];
            let mut playback_and_birds = tone_frame(start, 320, 220.0, 0.035);
            add_tone(&mut playback_and_birds, start, 1_050.0, 0.009);
            add_tone(&mut playback_and_birds, start, frequency, 0.13);
            assert!(detector.push(&playback_and_birds, true).is_none());
            assert!(!detector.speaking);
        }
    }

    #[test]
    fn modulated_speech_over_road_noise_still_opens_an_utterance() {
        let mut detector = UtteranceDetector::new(500);
        for frame in 0..30 {
            let start = frame * 320;
            let mut road = tone_frame(start, 320, 75.0, 0.035);
            add_tone(&mut road, start, 850.0, 0.005);
            assert!(detector.push(&road, false).is_none());
        }
        for frame in 30..50 {
            let start = frame * 320;
            let mut speech_over_road = tone_frame(start, 320, 75.0, 0.035);
            add_tone(&mut speech_over_road, start, 850.0, 0.005);
            let voice_amplitude = if frame % 4 == 0 { 0.12 } else { 0.055 };
            add_tone(&mut speech_over_road, start, 240.0, voice_amplitude);
            add_tone(
                &mut speech_over_road,
                start,
                1_450.0,
                voice_amplitude * 0.35,
            );
            assert!(detector.push(&speech_over_road, false).is_none());
            if detector.speaking {
                break;
            }
        }
        assert!(detector.speaking);
    }

    #[test]
    fn quieter_voice_over_loud_road_noise_still_opens_an_utterance() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        for frame in 0..30 {
            let start = frame * 320;
            let mut loud_road = tone_frame(start, 320, 75.0, 0.055);
            add_tone(&mut loud_road, start, 850.0, 0.008);
            assert!(detector.push(&loud_road, false).is_none());
        }
        // Silero confidence can be low for a distant driver masked by tire noise. Normal
        // capture uses a permissive fusion threshold instead of the strict playback threshold.
        detector.neural_speech_probability = Some(0.08);
        for frame in 30..65 {
            let start = frame * 320;
            let mut quiet_voice_in_car = tone_frame(start, 320, 75.0, 0.055);
            add_tone(&mut quiet_voice_in_car, start, 850.0, 0.008);
            let voice_amplitude = if frame % 4 == 0 { 0.055 } else { 0.030 };
            add_tone(&mut quiet_voice_in_car, start, 230.0, voice_amplitude);
            add_tone(
                &mut quiet_voice_in_car,
                start,
                1_400.0,
                voice_amplitude * 0.35,
            );
            assert!(detector.push(&quiet_voice_in_car, false).is_none());
            if detector.speaking {
                break;
            }
        }
        assert!(detector.speaking);
    }

    #[test]
    fn roadie_playback_is_not_user_speech_but_a_nearby_voice_is() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
        }

        for frame in 0..50 {
            let amplitude = if frame % 5 == 0 { 0.10 } else { 0.06 };
            let mut playback = tone_frame(frame * 320, 320, 210.0, amplitude);
            add_tone(&mut playback, frame * 320, 1_100.0, amplitude * 0.28);
            assert!(detector.push(&playback, true).is_none());
            assert!(!detector.take_playback_probe_request());
            assert!(!detector.speaking);
        }

        let mut requested_probe = false;
        for frame in 50..80 {
            let start = frame * 320;
            let mut playback_and_voice = tone_frame(start, 320, 210.0, 0.035);
            add_tone(&mut playback_and_voice, start, 1_100.0, 0.010);
            let voice_amplitude = if frame % 4 == 0 { 0.22 } else { 0.13 };
            add_tone(&mut playback_and_voice, start, 275.0, voice_amplitude);
            add_tone(
                &mut playback_and_voice,
                start,
                1_650.0,
                voice_amplitude * 0.4,
            );
            assert!(detector.push(&playback_and_voice, true).is_none());
            if detector.take_playback_probe_request() {
                requested_probe = true;
                break;
            }
        }
        assert!(requested_probe);
        detector.begin_playback_probe_confirmation();
        for frame in 0..2 {
            let mut echo_tail = tone_frame(frame * 320, 320, 210.0, 0.035);
            add_tone(&mut echo_tail, frame * 320, 1_100.0, 0.010);
            assert!(detector.push(&echo_tail, false).is_none());
        }
        for frame in 2..12 {
            let start = frame * 320;
            let voice_amplitude = if frame % 4 == 0 { 0.22 } else { 0.13 };
            let mut nearby_voice = tone_frame(start, 320, 275.0, voice_amplitude);
            add_tone(&mut nearby_voice, start, 1_650.0, voice_amplitude * 0.4);
            assert!(detector.push(&nearby_voice, false).is_none());
            if detector.speaking {
                break;
            }
        }
        assert!(detector.speaking);
        assert!(
            detector.utterance.len() <= WHISPER_SAMPLE_RATE as usize * 38 / 100,
            "Roadie-only playback lead-in should be trimmed before Whisper"
        );
    }

    #[test]
    fn realistic_quiet_voice_requests_an_immediate_playback_probe() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
        }

        for frame in 0..30 {
            let amplitude = if frame % 5 == 0 { 0.075 } else { 0.055 };
            let mut playback = tone_frame(frame * 320, 320, 210.0, amplitude);
            add_tone(&mut playback, frame * 320, 1_100.0, amplitude * 0.28);
            assert!(detector.push(&playback, true).is_none());
            assert!(
                !detector.take_playback_probe_request(),
                "Roadie's playback alone must not request a pause probe"
            );
        }

        let mut probe_frame = None;
        for frame in 30..45 {
            let start = frame * 320;
            let mut playback_and_quiet_voice = tone_frame(start, 320, 210.0, 0.055);
            add_tone(&mut playback_and_quiet_voice, start, 1_100.0, 0.015);
            let voice_amplitude = if frame % 4 == 0 { 0.050 } else { 0.032 };
            add_tone(&mut playback_and_quiet_voice, start, 275.0, voice_amplitude);
            add_tone(
                &mut playback_and_quiet_voice,
                start,
                1_650.0,
                voice_amplitude * 0.4,
            );
            assert!(detector.push(&playback_and_quiet_voice, true).is_none());
            if detector.take_playback_probe_request() {
                probe_frame = Some(frame);
                break;
            }
        }

        let probe_frame = probe_frame.unwrap_or_else(|| {
            panic!(
                "a nearby quiet voice should request a pause probe: metrics={:?}, playback raw={} band={}, probe samples={}, speaking={}",
                detector.last_metrics,
                detector.playback_noise_rms,
                detector.playback_noise_band_rms,
                detector.playback_probe_samples,
                detector.speaking
            )
        });
        assert!(
            probe_frame <= 35,
            "the speaker must duck within 120 ms of nearby speech"
        );
        assert!(
            !detector.speaking,
            "the pause probe must happen before semantic utterance confirmation"
        );
    }

    #[test]
    fn playback_probe_requires_neural_voice_after_roadie_goes_quiet() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        detector.begin_playback_probe_confirmation();
        detector.neural_speech_probability = Some(0.01);

        for frame in 0..24 {
            let start = frame * 320;
            let amplitude = if frame % 4 == 0 { 0.18 } else { 0.11 };
            let mut voice_shaped_echo = tone_frame(start, 320, 275.0, amplitude);
            add_tone(&mut voice_shaped_echo, start, 1_650.0, amplitude * 0.4);
            assert!(detector.push(&voice_shaped_echo, false).is_none());
        }

        assert!(!detector.speaking);
    }

    #[test]
    fn playback_probe_confirms_persistent_user_voice_after_echo_dies() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        detector.begin_playback_probe_confirmation();

        for frame in 0..2 {
            let mut echo_tail = tone_frame(frame * 320, 320, 210.0, 0.055);
            add_tone(&mut echo_tail, frame * 320, 1_100.0, 0.015);
            assert!(detector.push(&echo_tail, false).is_none());
            assert!(!detector.speaking);
        }

        for frame in 2..12 {
            let start = frame * 320;
            let amplitude = if frame % 3 == 0 { 0.050 } else { 0.032 };
            let mut nearby_voice = tone_frame(start, 320, 275.0, amplitude);
            add_tone(&mut nearby_voice, start, 1_650.0, amplitude * 0.4);
            assert!(detector.push(&nearby_voice, false).is_none());
            if detector.speaking {
                break;
            }
        }
        assert!(detector.speaking);
    }

    #[test]
    fn playback_probe_rejects_roadie_echo_that_ends_with_the_pause() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        detector.begin_playback_probe_confirmation();

        for frame in 0..2 {
            let mut echo_tail = tone_frame(frame * 320, 320, 210.0, 0.055);
            add_tone(&mut echo_tail, frame * 320, 1_100.0, 0.015);
            assert!(detector.push(&echo_tail, false).is_none());
        }
        for _ in 0..18 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
            assert!(!detector.speaking);
        }
    }

    #[test]
    fn playback_probe_guard_prevents_pause_resume_loops() {
        let active_probe = Some(PlaybackProbe {
            phase: PlaybackProbePhase::ConfirmingUser,
            started_at: Instant::now(),
        });
        assert!(should_begin_playback_probe(None, true, true, true));
        assert!(!should_begin_playback_probe(None, true, false, true));
        assert!(!should_begin_playback_probe(active_probe, true, true, true));
        assert!(!should_begin_playback_probe(None, false, true, true));
        assert!(!should_begin_playback_probe(None, true, true, false));
    }

    #[test]
    fn playback_barge_in_captures_a_full_utterance_for_direct_routing() {
        let mut detector = UtteranceDetector::new(500);
        detector.set_voice_isolation(true);
        for _ in 0..30 {
            assert!(detector.push(&vec![0.001; 320], false).is_none());
        }
        for frame in 0..50 {
            let amplitude = if frame % 5 == 0 { 0.10 } else { 0.06 };
            let mut playback = tone_frame(frame * 320, 320, 210.0, amplitude);
            add_tone(&mut playback, frame * 320, 1_100.0, amplitude * 0.28);
            assert!(detector.push(&playback, true).is_none());
            assert!(!detector.take_playback_probe_request());
        }

        let mut requested_probe = false;
        for frame in 50..80 {
            let start = frame * 320;
            let mut playback_and_voice = tone_frame(start, 320, 210.0, 0.035);
            add_tone(&mut playback_and_voice, start, 1_100.0, 0.010);
            let voice_amplitude = if frame % 4 == 0 { 0.22 } else { 0.13 };
            add_tone(&mut playback_and_voice, start, 275.0, voice_amplitude);
            add_tone(
                &mut playback_and_voice,
                start,
                1_650.0,
                voice_amplitude * 0.4,
            );
            assert!(detector.push(&playback_and_voice, true).is_none());
            if detector.take_playback_probe_request() {
                requested_probe = true;
                break;
            }
        }
        assert!(requested_probe);

        detector.begin_playback_probe_confirmation();
        for frame in 0..2 {
            let mut echo_tail = tone_frame(frame * 320, 320, 210.0, 0.035);
            add_tone(&mut echo_tail, frame * 320, 1_100.0, 0.010);
            assert!(detector.push(&echo_tail, false).is_none());
        }
        for frame in 2..52 {
            let start = frame * 320;
            let voice_amplitude = if frame % 4 == 0 { 0.18 } else { 0.11 };
            let mut nearby_voice = tone_frame(start, 320, 275.0, voice_amplitude);
            add_tone(&mut nearby_voice, start, 1_650.0, voice_amplitude * 0.4);
            assert!(detector.push(&nearby_voice, false).is_none());
        }
        assert!(
            detector.speaking,
            "persistent nearby voice should remain active until trailing silence"
        );
        let mut completed = None;
        for _ in 0..30 {
            if let Some(audio) = detector.push(&vec![0.0; 320], false) {
                completed = Some(audio);
                break;
            }
        }

        let audio = completed.expect("a close voice should reach semantic Whisper confirmation");
        assert!(audio.len() >= WHISPER_SAMPLE_RATE as usize * 4 / 5);
        assert!(!detector.speaking);
    }

    #[test]
    #[ignore = "manual replay harness: pipe mono 16 kHz f32le ride audio into stdin"]
    #[allow(
        clippy::cast_precision_loss,
        reason = "the diagnostic converts bounded recording frame counts to display seconds"
    )]
    fn replay_recorded_ride_through_detector() {
        use std::io::{ErrorKind, Read};

        const FRAME_SAMPLES: usize = 320;
        let mut detector = UtteranceDetector::new(600);
        if std::env::var_os("ROADIE_NEURAL_VAD").is_some() {
            detector.neural_vad = SileroVad::new().ok();
        }
        if std::env::var_os("ROADIE_VOICE_ISOLATION").is_some() {
            detector.set_voice_isolation(true);
        }
        let mut input = std::io::BufReader::new(std::io::stdin().lock());
        let mut bytes = [0_u8; FRAME_SAMPLES * size_of::<f32>()];
        let mut frame = 0_usize;
        let mut starts = 0_usize;
        let mut completed = 0_usize;
        let mut max_periodicity = 0.0_f32;
        let mut max_neural_probability = 0.0_f32;
        let mut max_candidate_samples = 0_usize;

        loop {
            match input.read_exact(&mut bytes) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => break,
                Err(error) => panic!("could not read replay audio: {error}"),
            }
            let samples = bytes
                .chunks_exact(size_of::<f32>())
                .map(|sample| f32::from_le_bytes(sample.try_into().unwrap()))
                .collect::<Vec<_>>();
            let was_speaking = detector.speaking;
            let utterance = detector.push(&samples, false);
            max_periodicity = max_periodicity.max(detector.last_metrics.voice_periodicity);
            max_neural_probability =
                max_neural_probability.max(detector.neural_speech_probability.unwrap_or_default());
            max_candidate_samples = max_candidate_samples.max(detector.candidate_samples);
            if !detector.speaking
                && detector.candidate_samples >= WHISPER_SAMPLE_RATE as usize * 16 / 100
            {
                println!(
                    "CANDIDATE,{:.2},ms={},voiced_ms={},periodicity={:.3},flatness={:.3},flat_min={:.3},zcr={:.3},raw={:.4},band={:.4},speech={:.4},treble={:.4},noise={:.4},band_noise={:.4}",
                    frame as f64 * 0.02,
                    detector.candidate_samples / 16,
                    detector.candidate_voiced_samples / 16,
                    detector.last_metrics.voice_periodicity,
                    detector.last_metrics.spectral_flatness,
                    detector.candidate_min_spectral_flatness,
                    detector.last_metrics.zero_crossing_rate,
                    detector.last_metrics.raw_rms,
                    detector.last_metrics.band_rms,
                    detector.last_metrics.speech_band_rms,
                    detector.last_metrics.treble_rms,
                    detector.noise_rms,
                    detector.noise_band_rms,
                );
            }
            if !was_speaking && detector.speaking {
                starts += 1;
                println!(
                    "START,{:.2},periodicity={:.3},flatness={:.3},zcr={:.3},raw={:.4},band={:.4},speech={:.4},treble={:.4},noise={:.4}",
                    frame as f64 * 0.02,
                    detector.last_metrics.voice_periodicity,
                    detector.last_metrics.spectral_flatness,
                    detector.last_metrics.zero_crossing_rate,
                    detector.last_metrics.raw_rms,
                    detector.last_metrics.band_rms,
                    detector.last_metrics.speech_band_rms,
                    detector.last_metrics.treble_rms,
                    detector.noise_rms,
                );
            }
            if let Some(audio) = utterance {
                completed += 1;
                println!(
                    "END,{:.2},{:.2}",
                    frame as f64 * 0.02,
                    audio.len() as f64 / f64::from(WHISPER_SAMPLE_RATE)
                );
            }
            frame += 1;
        }
        println!(
            "SUMMARY,{:.2},{starts},{completed},max_periodicity={max_periodicity:.3},max_neural={max_neural_probability:.3},max_candidate_ms={:.0}",
            frame as f64 * 0.02,
            max_candidate_samples as f64 / 16.0,
        );
    }

    #[test]
    #[ignore = "manual Whisper harness: set ROADIE_WHISPER_MODEL and pipe mono 16 kHz f32le"]
    fn classify_recorded_clip_with_whisper() {
        use std::io::Read;
        use whisper_rs::{WhisperContext, WhisperContextParameters};

        let mut bytes = Vec::new();
        std::io::stdin().lock().read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes.len() % size_of::<f32>(), 0);
        let audio = bytes
            .chunks_exact(size_of::<f32>())
            .map(|sample| f32::from_le_bytes(sample.try_into().unwrap()))
            .collect::<Vec<_>>();
        let model =
            std::env::var("ROADIE_WHISPER_MODEL").expect("ROADIE_WHISPER_MODEL is required");
        let mut parameters = WhisperContextParameters::default();
        parameters.use_gpu(true).flash_attn(true).gpu_device(0);
        let context = WhisperContext::new_with_params(&model, parameters).unwrap();
        let mut state = context.create_state().unwrap();
        let transcript = decode_transcript(&mut state, &audio, "en").unwrap();
        println!(
            "WHISPER,no_speech={:.4},lexical_tokens={},min_lexical_p={:.4},confident={},text={:?}",
            transcript.no_speech_probability,
            transcript.lexical_tokens,
            transcript.minimum_lexical_probability,
            transcript_is_confident(&transcript, "hey codex"),
            transcript.text
        );
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "small synthetic test-signal indexes are exactly representable enough for f32"
    )]
    fn tone_frame(start: usize, len: usize, frequency: f32, amplitude: f32) -> Vec<f32> {
        (start..start + len)
            .map(|sample| {
                let phase = 2.0 * std::f32::consts::PI * frequency * sample as f32
                    / WHISPER_SAMPLE_RATE as f32;
                phase.sin() * amplitude
            })
            .collect()
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "small synthetic test-signal indexes are exactly representable enough for f32"
    )]
    fn add_tone(samples: &mut [f32], start: usize, frequency: f32, amplitude: f32) {
        for (offset, sample) in samples.iter_mut().enumerate() {
            let phase = 2.0 * std::f32::consts::PI * frequency * (start + offset) as f32
                / WHISPER_SAMPLE_RATE as f32;
            *sample += phase.sin() * amplitude;
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "deterministic synthetic noise only needs f32-scale fidelity"
    )]
    fn pseudo_noise_frame(start: usize, len: usize, amplitude: f32) -> Vec<f32> {
        (start..start + len)
            .map(|index| {
                let mut value = u64::try_from(index).unwrap_or(0).wrapping_add(1);
                value ^= value << 13;
                value ^= value >> 7;
                value ^= value << 17;
                let normalized = (value & 0xffff) as f32 / 32_767.5 - 1.0;
                normalized * amplitude
            })
            .collect()
    }
}
