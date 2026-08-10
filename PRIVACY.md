# Privacy

Codex Roadie is designed without analytics, advertising, telemetry, or a Roadie-operated server.

## Data processed locally

- Microphone audio is captured through CoreAudio and transcribed locally with Whisper.
- Silero voice-activity detection and acoustic filtering run locally.
- Kokoro speech synthesis runs locally through MLX; Apple speech may be used as a fallback.
- Raw microphone audio is held in bounded memory for transcription and is not written to disk by the application.

## Data sent to third parties

- The final text prompt, selected working context, and coding-agent conversation are processed by the logged-in Codex or Claude Code CLI selected by the user. Their respective account terms and privacy policies apply.
- The first model setup contacts Hugging Face to download pinned Whisper/Kokoro/G2P artifacts.
- An optional SSH route sends coding-agent traffic to the host configured by the user.

Roadie does not collect credentials. It reuses the normal authentication state of installed CLIs and key-only SSH configuration.

## Data stored on the Mac

Roadie stores settings, coding-agent session identifiers, token-usage totals, and any request waiting for reconnect under `~/Library/Application Support/dev.codexroadie.desktop/`. A completed or cancelled request is removed from reconnect state. Conversation messages are not persisted as a Roadie chat archive.

Deleting that Application Support directory removes Roadie's local settings and recovery state. Model files may also exist in the Hugging Face cache under the user's home directory.

## Public bug reports

Do not attach Application Support data, raw recordings, private source code, authentication files, session identifiers, or unsanitized diagnostics to an issue. Reduce a report to the smallest non-sensitive reproduction.
