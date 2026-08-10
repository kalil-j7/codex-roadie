#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
app="$root/target/release/bundle/macos/Codex Roadie.app"
metallib="$root/tts-helper/bin/mlx.metallib"

if [[ ! -d "$app" ]] || [[ ! -f "$metallib" ]]; then
  echo "The app bundle or MLX shader library is missing." >&2
  exit 1
fi

cp "$metallib" "$app/Contents/MacOS/mlx.metallib"
codesign --force --deep --sign - "$app"
codesign --verify --deep --strict "$app"
echo "Bundled MLX shaders: $app/Contents/MacOS/mlx.metallib"
