#!/usr/bin/env bash
set -euo pipefail

readonly model_name="ggml-large-v3-turbo-q5_0.bin"
readonly expected_sha256="394221709cd5ad1f40c46e6031ca61bce88931e6e088c188294c6d5a55ffa7e2"
readonly model_url="https://huggingface.co/ggerganov/whisper.cpp/resolve/main/${model_name}?download=true"
readonly model_dir="${CODEX_ROADIE_MODEL_DIR:-$HOME/Library/Application Support/dev.codexroadie.desktop/models}"
readonly model_path="$model_dir/$model_name"
readonly partial_path="$model_path.partial"

for command in curl shasum; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Missing required command: $command" >&2
    exit 1
  fi
done

checksum() {
  shasum -a 256 "$1" | awk '{print $1}'
}

mkdir -p "$model_dir"
if [[ -f "$model_path" ]]; then
  if [[ "$(checksum "$model_path")" == "$expected_sha256" ]]; then
    echo "Whisper model ready: $model_path"
    exit 0
  fi
  mv "$model_path" "$model_path.invalid-$(date +%s)"
fi

echo "Downloading local Whisper large-v3-turbo Q5 model (about 547 MiB)..."
curl --fail --location --retry 3 --continue-at - --output "$partial_path" "$model_url"

actual_sha256="$(checksum "$partial_path")"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "Whisper model checksum mismatch: expected $expected_sha256, got $actual_sha256" >&2
  exit 1
fi

mv "$partial_path" "$model_path"
echo "Whisper model ready: $model_path"
