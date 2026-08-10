# Third-party notices

Codex Roadie depends on third-party software and model weights. The dependency lockfiles are authoritative for exact package versions; this file records the directly integrated audio components and downloadable artifacts.

| Component | Pinned version or revision | License | Source |
| --- | --- | --- | --- |
| whisper-rs / whisper-rs-sys | 0.16.0 / 0.15.0 | Unlicense | https://github.com/tazz4843/whisper-rs |
| whisper.cpp | Through whisper-rs | MIT | https://github.com/ggml-org/whisper.cpp |
| Whisper large-v3-turbo Q5 weights | SHA-256 pinned by installer | MIT | https://huggingface.co/ggerganov/whisper.cpp |
| silero-vad-crs | 0.4.0 | MIT | https://crates.io/crates/silero-vad-crs |
| Silero VAD | Embedded through silero-vad-crs | MIT | https://github.com/snakers4/silero-vad |
| mlx-audio-swift | 0.1.3 | MIT | https://github.com/Blaizzy/mlx-audio-swift |
| MLX Swift | Resolved in `Package.resolved` | MIT | https://github.com/ml-explore/mlx-swift |
| Swift Hugging Face | 0.9.0 | Apache-2.0 | https://github.com/huggingface/swift-huggingface |
| Kokoro-82M-bf16 | `a71e4d38b236d968966a2002c4c895dbd12b1c3c` | Apache-2.0 | https://huggingface.co/mlx-community/Kokoro-82M-bf16 |
| KittenTTS English G2P resources | `9c692b92682d959d9013a9cfe6a49541997add18` | MIT | https://huggingface.co/beshkenadze/kitten-tts-g2p |
| avspeechsynthesizer-rs | 0.3.7 | MIT OR Apache-2.0 | https://crates.io/crates/avspeechsynthesizer-rs |

The application does not redistribute the Whisper, Kokoro, or G2P model files in this repository. They are downloaded at runtime or installation time from their upstream sources. Binary distributors are responsible for preserving all notices required by the complete transitive dependency set.

OpenAI, Codex, Anthropic, and Claude names are used only to identify compatible third-party services and tools. This project is not affiliated with or endorsed by those companies.
