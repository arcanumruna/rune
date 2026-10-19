#!/usr/bin/env bash
# Updates internal @${CONAN_USER}/${CONAN_CHANNEL} dependencies in conandata.yml to match
# the latest released version (from git tags) of each sibling project.
#
# Usage: ./update-deps.sh
set -euo pipefail
: "${CONAN_USER:?set CONAN_USER (conan package user)}"
: "${CONAN_CHANNEL:=dev}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
CONANDATA="$SCRIPT_DIR/conandata.yml"

if [[ ! -f "$CONANDATA" ]]; then
  echo "No conandata.yml found — nothing to update."
  exit 0
fi

# Packages to skip (third-party forks with manual versioning)
SKIP_PKGS="etcd-cpp-apiv3"

# Map conan package name → directory name
pkg_to_dir() {
  case "$1" in
    etcd-cpp-apiv3) echo "etcdv3-fork" ;;
    *) echo "$1" ;;
  esac
}

# Extract internal package names from conandata.yml
PKGS=$(sed -n 's/.*"\([a-z0-9_-]*\)\/[^"]*@${CONAN_USER}\/${CONAN_CHANNEL}".*/\1/p' "$CONANDATA" | sort -u)

CHANGED=false
for pkg in $PKGS; do
  # Skip third-party forks
  if echo "$SKIP_PKGS" | grep -qw "$pkg"; then
    echo "⏭️  Skipping $pkg (third-party)"
    continue
  fi

  dir=$(pkg_to_dir "$pkg")
  proj_dir="$PARENT_DIR/$dir"

  if [[ ! -d "$proj_dir/.git" ]]; then
    echo "⚠️  Skipping $pkg — no git repo at $proj_dir"
    continue
  fi

  # Get latest tag (e.g. v1.0.0) and strip the 'v' prefix
  latest_tag=$(git -C "$proj_dir" describe --tags --abbrev=0 2>/dev/null || echo "")
  if [[ -z "$latest_tag" ]]; then
    echo "⚠️  Skipping $pkg — no tags found"
    continue
  fi
  release_version="${latest_tag#v}"

  # Check if already up to date
  current=$(sed -n "s/.*\"${pkg}\/\([^\"]*\)@${CONAN_USER}\/${CONAN_CHANNEL}\".*/\1/p" "$CONANDATA")
  if [[ "$current" == "$release_version" ]]; then
    echo "✔️  $pkg already at $release_version"
    continue
  fi

  # Replace in conandata.yml
  sed -i '' "s|\"${pkg}/[^\"]*@${CONAN_USER}/${CONAN_CHANNEL}\"|\"${pkg}/${release_version}@${CONAN_USER}/${CONAN_CHANNEL}\"|g" "$CONANDATA"
  echo "✅ $pkg $current → $release_version"
  CHANGED=true
done

if [[ "$CHANGED" == "true" ]]; then
  echo "Dependencies updated."
else
  echo "All dependencies already up to date."
fi
