#!/usr/bin/env bash
# Usage: broker/build_docker.sh [extra docker buildx build args...]
#
# Builds the published image (the broker plus the `rune` CLI: the Dockerfile's `release` target)
# as rune:<VERSION.txt>. With no extra args the image is loaded
# into the local docker; CI passes --push/--tag/--cache-* instead (see .github/workflows/broker-image.yml).
#
# Env:
#   IMAGE_NAME                 image name (default rune)
#   FLIGHT_RECORDER_DIR        flight-recorder checkout (default: sibling of the repo root)
#   CMAKE_BUILD_PARALLEL_LEVEL cap on compile jobs (each big TU needs ~1.5 GB)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -f VERSION.txt ]]; then
  echo "❌ VERSION.txt not found"
  exit 1
fi
VERSION=$(< VERSION.txt)
if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-snapshot)?$ ]]; then
  echo "❌ Invalid version format: '$VERSION'"
  exit 1
fi

IMAGE_NAME="${IMAGE_NAME:-rune}"
FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-../../flight-recorder}"
if [[ ! -d $FLIGHT_RECORDER_DIR/src || ! -d $FLIGHT_RECORDER_DIR/include ]]; then
  echo "❌ flight-recorder not found at '$FLIGHT_RECORDER_DIR' (set FLIGHT_RECORDER_DIR)"
  exit 1
fi

ARGS=("$@")
[[ ${#ARGS[@]} -eq 0 ]] && ARGS=(--load)

echo "🛠️  Building '$IMAGE_NAME:$VERSION'..."
docker buildx build \
  --file docker/Dockerfile \
  --target release \
  --build-context rune-protocol=../protocol \
  --build-context rune-codec=../codec \
  --build-context rune-crypto=../crypto \
  --build-context rune-client=../client-cpp \
  --build-context rune-ql=../ql \
  --build-context flight-recorder="$FLIGHT_RECORDER_DIR" \
  --build-arg CMAKE_BUILD_PARALLEL_LEVEL \
  --label org.opencontainers.image.version="$VERSION" \
  --label org.opencontainers.image.revision="$(git rev-parse HEAD)" \
  --tag "$IMAGE_NAME:$VERSION" \
  --progress=plain \
  "${ARGS[@]}" \
  .

echo "✅ Built $IMAGE_NAME:$VERSION"
