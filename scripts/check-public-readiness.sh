#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

readonly excludes=(
  --glob '!target/**'
  --glob '!node_modules/**'
  --glob '!tts-helper/.build/**'
  --glob '!tts-helper/bin/**'
  --glob '!scripts/check-public-readiness.sh'
)

if rg -n --hidden "${excludes[@]}" \
  '(/Users/kalil|/home/kcj|\bkcj\b|\bpopos\b|joy-in-movement|ultimate-energy-visualizer|Kalil iPhone|com\.kcj|100\.105\.28\.4|REAPER Media)' .; then
  echo "Personal machine or project details remain in publishable files." >&2
  exit 1
fi

if rg -n --hidden "${excludes[@]}" \
  '(-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9_]{20,}|sk-[A-Za-z0-9_-]{20,}|xox[baprs]-[A-Za-z0-9-]{10,}|(AWS_SECRET_ACCESS_KEY|ANTHROPIC_API_KEY|OPENAI_API_KEY|SUPABASE_ACCESS_TOKEN|SUPABASE_DB_PASSWORD)[[:space:]]*[:=][[:space:]]*[A-Za-z0-9_./+=-]{8,})' .; then
  echo "A credential-shaped value remains in publishable files." >&2
  exit 1
fi

for required in LICENSE README.md PRIVACY.md SECURITY.md CONTRIBUTING.md THIRD_PARTY_NOTICES.md; do
  if [[ ! -s "$required" ]]; then
    echo "Missing required public repository file: $required" >&2
    exit 1
  fi
done

while IFS= read -r path; do
  echo "Forbidden generated, credential, model, or recording file: $path" >&2
  exit 1
done < <(
  find . -type f \
    -not -path './target/*' \
    -not -path './node_modules/*' \
    -not -path './tts-helper/.build/*' \
    -not -path './tts-helper/bin/*' \
    \( -name '.env' -o -name '.env.*' -o -name '*.pem' -o -name '*.key' \
       -o -name '*.p12' -o -name '*.mobileprovision' -o -name '*.wav' \
       -o -name '*.aiff' -o -name '*.m4a' -o -name '*.mp3' -o -name '*.flac' \
       -o -name '*.onnx' -o -name '*.bin' \) -print
)

echo "Public-readiness checks passed."
