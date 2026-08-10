# Codex Roadie

Codex Roadie is an unofficial, open-source macOS voice companion for local or SSH-hosted coding-agent sessions. It combines CoreAudio capture, embedded Silero VAD, local Whisper transcription on Metal, Rust orchestration, optional Codex or Claude Code execution, and a fixed local Kokoro voice.

This community project is not affiliated with, sponsored by, or endorsed by OpenAI or Anthropic. Codex, OpenAI, Claude, and Anthropic are trademarks of their respective owners.

> **Alpha software:** Roadie can execute coding-agent commands without interactive approval prompts. Start with the default workspace-write access, configure it while parked, and review the [security model](#security-model) before enabling Full Mac access.

## Platform

- Apple Silicon Mac
- macOS 14 or newer
- Microphone permission
- Internet access for the selected coding agent and first-run model downloads

Audio transcription and speech synthesis run locally. Prompts and agent responses pass through whichever logged-in CLI you select. Roadie includes no analytics, advertising, or telemetry. See [PRIVACY.md](PRIVACY.md) for the complete data flow.

## Features

- Persistent CoreAudio microphone capture independent of Siri and `SFSpeechRecognizer`.
- Local Whisper `large-v3-turbo-q5_0` transcription through whisper.cpp and Metal.
- Embedded Silero VAD plus road-rumble, impact, bird, window-motor, and playback-echo rejection.
- Voice Isolation and unprocessed-microphone detector profiles.
- Natural barge-in: nearby speech can pause playback and become an addition without another wake phrase.
- Mac-local Codex execution or optional key-only SSH execution on any host with Bash and Codex installed.
- Optional Mac-local Claude Code worker when Full Mac access is explicitly enabled.
- User-selectable Codex model, reasoning effort, service tier, workspace, and access level.
- Up to three independent workers for explicitly parallel requests.
- Persistent turn recovery with queued additions and capped reconnect backoff.
- Local Kokoro 82M speech through MLX with one fixed `af_sarah` voice and Apple speech fallback.
- Observable Capture → Whisper → Mac/SSH → Worker → Reporter → Voice pipeline and token usage.

## Voice commands

| Say | Result |
| --- | --- |
| “Hey Codex, inspect the current changes” | Starts a Codex turn |
| “Hey Claude, inspect the current changes” | Starts the optional Claude Code worker |
| “Hey Codex” … “run the tests” | Uses the eight-second armed follow-up window |
| “Hey Codex, status” | Speaks the current worker state |
| “Hey Codex, cancel” | Interrupts all active turns and clears saved reconnect work |
| “Hey Codex, stop speaking” | Stops speech playback immediately |
| “Hey Codex, repeat” | Replays the last agent message |
| “Hey Codex, new session” | Clears the active coding-agent session |
| “Hey Codex, set Mac workdir to /Users/example/sample-project” | Validates and persists a new Mac workspace |
| “Hey Codex, in parallel, inspect the logs” | Starts an independent worker if a slot is available |
| “Hey Codex, worker two, use reference four” | Sends an addition to a named worker |

The wake matcher accepts common Whisper variants such as “Hey Code X,” “Hey Codecs,” and “Hey Kodex.” Ordinary speech without a wake phrase remains ambient unless Roadie has just paused its own playback for a verified barge-in.

## Build

Install these prerequisites:

- Xcode and Xcode Command Line Tools with Swift 6.2+
- Rust 1.90+
- Node.js 22+ and npm
- CMake
- Codex CLI installed and logged in
- Optional: Claude Code, ADB, and an SSH client/host
- About 12 GiB free for a clean build; runtime models use about 1.2 GiB

Then run:

```bash
git clone https://github.com/kalil-j7/codex-roadie.git
cd codex-roadie
./scripts/build-macos.sh --install
open "$HOME/Applications/Codex Roadie.app"
```

Without `--install`, the bundle stays under `target/release/bundle/macos/`.

The installer downloads Whisper `ggml-large-v3-turbo-q5_0.bin` and verifies its SHA-256 checksum. On first launch, the native helper downloads pinned revisions of Kokoro and its English G2P resources, then warms the MLX kernels. Siri and Apple Speech Recognition permission are not required.

## First-run configuration

Roadie defaults to:

- Mac-local Codex
- the logged-in user's home directory
- the Codex CLI's default model and reasoning effort
- standard service tier
- workspace-write access
- microphone autostart disabled
- Voice Isolation detector profile enabled

Open settings, choose a real workspace, and use **Save & test**. Device tools are optional and do not make the basic Mac worker test fail.

### Optional SSH worker

Roadie never stores SSH passwords and always uses `BatchMode=yes`. Configure key-only authentication before selecting the SSH worker:

```bash
ssh-keygen -t ed25519 -f "$HOME/.ssh/codex-roadie" -C "codex-roadie-mac"
```

Add the public key to the remote account and create a generic alias:

```sshconfig
Host roadie-build
  HostName YOUR_REMOTE_HOST_OR_PRIVATE_IP
  User developer
  IdentityFile ~/.ssh/codex-roadie
  IdentitiesOnly yes
```

Verify the exact route without a password prompt:

```bash
ssh -o BatchMode=yes roadie-build \
  'bash -lc "cd /home/developer/projects/example-app && codex --version"'
```

Enter the alias, remote workspace, and remote Codex executable in Roadie settings. The remote host can be any system with Bash and Codex; no particular Linux distribution is required.

## Model and reporter behavior

Leave **MODEL** and **EFFORT** on their defaults for maximum account portability, or choose a model available to your Codex account. Roadie does not bundle access to paid models or services.

**Relay** sends worker chat updates to a separate, read-only local Codex reporter for concise narration. Relay is optional and disabled by default. If a configured reporter model is unavailable, Roadie tries its configured fallback and then speaks the raw worker update.

Claude Code is optional. Because this integration uses Claude's non-interactive permission bypass, Roadie refuses to launch it unless the user explicitly selects **Full Mac access**.

## Connectivity recovery

Before starting a worker, Roadie atomically persists the active request. Additions spoken during a disconnect join that saved request. Reconnect attempts use exponential backoff with jitter, cap at 30 seconds, resume the same session when possible, and instruct the worker to inspect current state before repeating any side effect.

Local microphone capture, control phrases, and TTS remain available while the network is down. Cancellation deletes the saved request and stops reconnecting.

## In-car use

Complete setup while parked. Secure the Mac, keep it powered, and do not interact with its screen while driving. A directional Bluetooth microphone or AirPods usually rejects cabin playback and road noise better than the Mac's internal microphone.

When macOS Voice Isolation is enabled, leave Roadie's matching setting on. Disable that setting for an unprocessed microphone so the stricter road-noise detector is used.

## Security model

- **Workspace write** is the default for local and SSH Codex workers.
- **Read only** is available for inspection-only sessions.
- **Full Mac access** is an explicit opt-in with a confirmation warning. It uses `danger-full-access` and non-interactive approvals, allowing the selected agent to operate anything available to the logged-in macOS account.
- Claude Code is rejected unless Full Mac access was explicitly selected.
- SSH uses key-only, non-interactive authentication and shell-quotes all remote paths.
- Reporter turns receive no task authority and run read-only.
- Changing host, engine, workspace, model, effort, or access starts a fresh host-tagged session.

Full access may be necessary for ADB or CoreSimulator sockets, but it should not be enabled for ordinary repository work.

## Privacy and local state

Settings, session identifiers, usage totals, and any interrupted request are stored under:

```text
~/Library/Application Support/dev.codexroadie.desktop/
```

Reset microphone permission with:

```bash
tccutil reset Microphone dev.codexroadie.desktop
```

Do not attach that Application Support directory, recordings, authentication files, or unsanitized diagnostics to public issues.

## Verification

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
node --check ui/app.js
npm audit
./scripts/check-public-readiness.sh
```

The portable Rust core can be tested on any Rust-supported platform with `cargo test -p roadie-core`. The complete application, CoreAudio/Metal paths, MLX helper, and `.app` bundle require an Apple Silicon Mac.

## Model provenance

- Whisper model: `ggml-large-v3-turbo-q5_0.bin`, checksum pinned in `scripts/install-whisper-model.sh`
- Kokoro MLX model: `mlx-community/Kokoro-82M-bf16` at commit `a71e4d38b236d968966a2002c4c895dbd12b1c3c`
- English G2P resources: `beshkenadze/kitten-tts-g2p` at commit `9c692b92682d959d9013a9cfe6a49541997add18`

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for licenses and attribution.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and the safety expectations in [VOICE_APP_TRANSFER_CHECKLIST.md](VOICE_APP_TRANSFER_CHECKLIST.md). The project is licensed under the [MIT License](LICENSE).
