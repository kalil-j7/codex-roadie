# Car Voice App Transfer Checklist

Use this checklist when transferring Codex Roadie's voice architecture to the in-car music mixing app.

## 1. Define the real-time state machine first

- Keep explicit states for loading, listening, capturing, transcribing, transferring, processing, reporting, speaking, paused, and error.
- Make start, stop, cancel, and shutdown idempotent. Never start a second microphone engine or permission poller while one is active.
- Give every capture and remote turn a generation ID. Ignore late events from older generations.
- Persist stable settings, session identifiers, and any in-flight user request that must survive connectivity loss; rebuild audio/process handles after launch or wake.

## 2. Make audio capture independent of Siri

- Request only macOS microphone permission and provide `NSMicrophoneUsageDescription`.
- Use Core Audio/AVAudioEngine capture directly. Do not use `SFSpeechRecognizer`, so Siri being disabled cannot break transcription.
- Ask for permission once, surface denied/restricted states, and link to System Settings. Do not restart the permission daemon in a loop.
- Keep one long-lived audio engine where practical and handle device changes, sleep/wake, route loss, Bluetooth reconnects, and sample-rate changes.
- Convert captured audio to the model's exact mono sample rate in a bounded ring buffer; never block the real-time audio callback.

## 3. Ship local transcription as a managed asset

- Use Whisper `large-v3-turbo` through a native Rust backend with Metal acceleration on Apple Silicon.
- Download or bundle a version-pinned model, verify its checksum, report download/load/warm-up progress, and fail with a useful recovery action.
- Warm the model before the first command and reuse the loaded context. Do not reload it per utterance.
- Show live partial capture separately from finalized Whisper text so users know what the microphone heard versus what was committed.
- Add vocabulary hints/post-corrections for product terms, artist names, mixer controls, “Codex/Kodex,” and command phrases without silently rewriting arbitrary speech.

## 4. Tune detection for a moving car

- Maintain an adaptive noise floor instead of one fixed amplitude threshold.
- Require voice-like modulation across multiple frames; reject stationary road/engine noise and short tonal bird chirps.
- Use pre-roll, attack/release hysteresis, minimum speech duration, and a bounded silence timeout so initial consonants are retained.
- Keep separate thresholds for idle listening and barge-in while the app is speaking or music is playing.
- Test windows-open, HVAC, rain, tires on different road surfaces, birds, passengers, and music at realistic volume.
- Store only opt-in diagnostic clips; default diagnostics should be levels/features, not raw cabin audio.

## 5. Implement real barge-in

- Keep listening while TTS or music is active and feed the known playback signal into echo cancellation or an echo-reference detector.
- On probable nearby speech, pause—not destroy—playback and capture the utterance.
- If the utterance is not addressed to the app, resume from the paused position. If it is a command, stop or duck playback and execute it.
- Prioritize safety commands such as cancel, stop, pause music, and mute before free-form prompts.
- Add a short acknowledgement only after the command is committed; avoid acknowledgements that mask the user's next words.

## 6. Separate intent parsing from transcription

- Normalize punctuation and common Whisper variants while retaining the original text sent to the worker.
- Support wake-only arming plus a short follow-up window, full wake-and-command phrases, and explicit controls.
- At minimum test: start/request, add/change request, cancel task, stop speaking, pause/resume music, status, repeat, and new session.
- Recognize “Hey Codex cancel” and common variants deterministically; do not ask a model whether an emergency control should run.
- Treat a command spoken during active work as an addition unless it matches a control intent.

## 7. Use Codex's active-turn controls

- Run `codex app-server --stdio` directly for the Mac execution worker; let that worker use a separate key-only SSH process when source/build work lives remotely.
- Initialize, start or resume a thread, start the turn, and wait for the `turn/started` notification before steering.
- Send additions through `turn/steer` with the exact `threadId` and `expectedTurnId`; do not cancel and restart the work.
- Send cancellation through `turn/interrupt`, then force-kill the process only after a short timeout.
- Keep additions queued until the turn is steerable, reject stale turn IDs, and start a new turn if the prior one has already completed.
- Stream item, usage, error, and completion events into the UI; never infer “SSH blocker” from an ordinary worker limitation.

## 8. Make SSH non-interactive and diagnosable

- Install a dedicated key with `ssh-copy-id` from the controlling Mac and verify `ssh -o BatchMode=yes <host> true`.
- Configure the alias, user, hostname/Tailscale address, identity file, and host-key policy in `~/.ssh/config`.
- Never depend on a GUI password prompt: background app processes do not have a safe interactive terminal.
- Validate the remote workspace, executable, Codex login, model availability, and writable sandbox before driving.
- Classify failures separately: DNS, route, host key, authentication, remote executable, app-server protocol, model/API, tool permission, and task-level blockers.
- Use timeouts and cancellation for connection, startup, steering, and shutdown; never leave orphaned SSH/Codex processes.

### Connectivity recovery

- Save the original request before network transfer and keep unacknowledged additions in a durable local queue.
- Distinguish transient DNS/route/reset/timeout/502–504 failures from authentication, configuration, policy, and task failures; retry only the transient class.
- Use exponential backoff with jitter and a reasonable cap. Keep cancellation and offline additions responsive while waiting instead of sleeping the coordinator thread.
- Do not depend on socket/process exit alone: independently probe the service route during active work and add a bounded stream-silence watchdog so half-open connections are recycled.
- Resume the same persisted thread after reconnect. Tell the worker to inspect existing state and never repeat completed external side effects.
- Remove durable pending work only after confirmed completion, explicit cancellation, or a terminal non-network failure. Restore it automatically after an app restart.
- Treat the protocol's successful turn-completion event as the durable commit point; never wait for child-process teardown or reporter drain before clearing completed work.
- Show an offline-safe state, attempt number, retry timing, and queued-addition count without repeatedly speaking the same warning.

## 9. Keep the reporter and worker responsibilities clear

- Let the deep execution worker own decisions, tools, and file changes; place it on the machine that owns the device/UI under test.
- Let a fast local reporter turn concrete worker events into short spoken updates without inventing blockers or exposing hidden reasoning.
- Preserve exact completed results and required user actions. Coalesce noisy updates and interrupt obsolete speech when fresher information arrives.
- Show and account for worker usage separately from reporter usage.

## 10. Budget latency end to end

- Measure voice onset, utterance finalization, Whisper start/end, worker-process/SSH start, prompt transfer, turn start, first worker event, TTS first audio, and completion.
- Run safe independent work concurrently: SSH warm-up, local model warm-up, asset verification, and UI initialization.
- Stream audio and text where supported; do not wait for the entire spoken response before playback.
- Keep audio callbacks lock-free/bounded and move resampling, Whisper, networking, and TTS generation off the real-time thread.
- Treat three seconds as a first-response target, not a guaranteed completion time for deep work.

## 11. Make local TTS production-safe

- Version-pin the local voice model/runtime, verify checksums, bundle required Metal shaders, and warm it during startup.
- Stream generated audio, expose generation progress, and retain a reliable system-voice fallback.
- Support pause, resume, stop, queue replacement, route change, and Bluetooth output loss.
- Duck music smoothly during acknowledgements/reports and restore the previous gain; never change the user's master volume unexpectedly.

## 12. Expose observable recovery in the UI

- Show microphone level, partial capture, final transcript, transcription progress, transfer status, active model/effort/fast mode, session ID, usage, and reconnect state.
- While a turn is active, label the text action “Add,” not “Send.” Keep Cancel independently available.
- Explain permission, model, SSH, and remote errors with one concrete recovery step and retain detailed diagnostics outside spoken output.
- Avoid retry loops that flicker the menu-bar microphone indicator or repeatedly reopen devices.

## 13. Test with deterministic fixtures and the real car

- Unit-test intent variants, VAD/noise fixtures, resampling, stale generations, state transitions, queueing, cancel, and turn steering.
- Integration-test microphone lifecycle, model load failure, Bluetooth/device changes, local device sockets, SSH disconnect/reconnect, host-tagged stale sessions, and app-server error responses.
- Run recorded fixtures for road noise, birds, HVAC, music leakage, playback echo, quiet speech, accented speech, and overlapping passengers.
- Prove live `turn/steer` and `turn/interrupt` against the exact Mac execution-worker Codex version before release.
- Finish with parked and passenger-operated on-road QA; never tune or debug the interface while driving.

## 14. Package and release reproducibly

- Build a signed arm64 macOS app containing every helper, model metadata file, dynamic library, and Metal shader it needs.
- Verify code signing, hardened runtime/entitlements, microphone usage text, quarantine behavior, and a clean-machine launch.
- Preserve user settings across upgrades, migrate schemas explicitly, and provide a model-cache reset that does not delete unrelated data.
- Pin tool/runtime/model versions, record checksums, and keep an automated release smoke test for capture, transcription, TTS, SSH, steer, cancel, and shutdown.
