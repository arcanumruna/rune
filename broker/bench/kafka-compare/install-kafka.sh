#!/usr/bin/env bash
# Install a JDK + Kafka release on the bench box. Idempotent.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"

if ! command -v java >/dev/null 2>&1; then
  echo "installing openjdk-17-jre-headless ..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq openjdk-17-jre-headless >/dev/null
fi
java -version

TGZ="kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
if [ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
  echo "downloading $TGZ ..."
  cd /tmp
  for base in "https://downloads.apache.org/kafka" "https://archive.apache.org/dist/kafka"; do
    curl -fsSL "$base/${KAFKA_VERSION}/${TGZ}" -o "$TGZ" && break || true
  done
  [ -s "$TGZ" ] || { echo "kafka download failed"; exit 1; }
  rm -rf "$KAFKA_HOME"
  mkdir -p "$KAFKA_HOME"
  tar xzf "$TGZ" -C "$KAFKA_HOME" --strip-components=1
  rm -f "$TGZ"
fi
echo "kafka at: $KAFKA_HOME"
ls "$KAFKA_HOME/bin" | grep -E 'perf-test|e2e-latency|server-start' || true
echo "install-kafka: OK"
