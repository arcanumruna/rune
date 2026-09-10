#!/usr/bin/env bash
set -euo pipefail

ARTIFACTORY_HOST="${ARTIFACTORY_HOST:-}"
CONAN_PASSWORD="${CONAN_PASSWORD:-}"

docker build \
  --build-arg ARTIFACTORY_HOST="${ARTIFACTORY_HOST}" \
  --build-arg CONAN_PASSWORD="${CONAN_PASSWORD}" \
  -t rune-broker:latest .
