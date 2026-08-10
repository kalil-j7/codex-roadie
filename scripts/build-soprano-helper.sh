#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]] || [[ "$(uname -m)" != "arm64" ]]; then
  echo "The Kokoro helper requires an Apple Silicon Mac." >&2
  exit 1
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
helper="$root/tts-helper"
metal_source="$helper/.build/checkouts/mlx-swift/Source/Cmlx/mlx"
metal_build="$helper/.build/mlx-metal"

swift build --package-path "$helper" -c release --product roadie-tts
if [[ ! -d "$metal_source" ]]; then
  echo "MLX shader sources were not found after the Swift build." >&2
  exit 1
fi
cmake \
  -S "$metal_source" \
  -B "$metal_build" \
  -DMLX_BUILD_TESTS=OFF \
  -DMLX_BUILD_BENCHMARKS=OFF \
  -DMLX_BUILD_EXAMPLES=OFF \
  -DMLX_BUILD_GGUF=OFF \
  -DCMAKE_BUILD_TYPE=Release
cmake --build "$metal_build" --target mlx-metallib --parallel
metallib="$(find "$metal_build" -name mlx.metallib -type f -print -quit)"
if [[ -z "$metallib" ]]; then
  echo "The MLX Metal shader library was not produced." >&2
  exit 1
fi
mkdir -p "$helper/bin"
cp "$helper/.build/release/roadie-tts" "$helper/bin/roadie-tts-aarch64-apple-darwin"
cp "$metallib" "$helper/bin/mlx.metallib"
chmod +x "$helper/bin/roadie-tts-aarch64-apple-darwin"

echo "Built Kokoro helper and MLX shaders under: $helper/bin"
