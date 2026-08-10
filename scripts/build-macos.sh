#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]] || [[ "$(uname -m)" != "arm64" ]]; then
  echo "Codex Roadie must be bundled on macOS because it uses Metal, CoreAudio, and AVFAudio." >&2
  echo "The current neural TTS helper supports Apple Silicon only." >&2
  exit 1
fi

for command in cargo npm node xcodebuild swift cmake curl shasum ditto; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Missing required command: $command" >&2
    exit 1
  fi
done

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

npm ci
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
node --check ui/app.js
./scripts/check-public-readiness.sh
npm run build:mac

app="$root/target/release/bundle/macos/Codex Roadie.app"
if [[ ! -d "$app" ]]; then
  echo "Build finished, but the app bundle was not found at: $app" >&2
  exit 1
fi

if [[ "${1:-}" == "--install" ]]; then
  "$root/scripts/install-whisper-model.sh"
  destination="$HOME/Applications/Codex Roadie.app"
  mkdir -p "$HOME/Applications"
  ditto "$app" "$destination"
  echo "Installed: $destination"
else
  echo "Built: $app"
fi
