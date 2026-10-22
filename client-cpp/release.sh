#!/usr/bin/env bash
# Usage: client-cpp/release.sh [major|minor|patch]
#
# Cuts a client-cpp release from main: VERSION.txt X.Y.Z-snapshot -> X.Y.Z, tags client-cpp-vX.Y.Z,
# sets the next snapshot and pushes both commits and the tag. The tag push triggers
# .github/workflows/client-cpp-release.yml, which builds and tests the Conan package, uploads it to the
# Conan remote and publishes a GitHub Release with a source tarball.
# The "client-cpp-" prefix keeps client tags apart from the other components' tags in this monorepo.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

RELEASE_TYPE="${1:?Usage: client-cpp/release.sh [major|minor|patch]}"
VERSION=$(< VERSION.txt)

if [[ ! $VERSION =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)-snapshot$ ]]; then
  echo "❌ VERSION.txt must be X.Y.Z-snapshot, got: $VERSION"
  exit 1
fi

BRANCH=$(git branch --show-current)
if [[ $BRANCH != main ]]; then
  echo "❌ Releases are cut from main, current branch: ${BRANCH:-detached HEAD}"
  exit 1
fi
if [[ -n $(git status --porcelain) ]]; then
  echo "❌ Working tree is not clean"
  exit 1
fi
git fetch -q origin main
if [[ $(git rev-parse HEAD) != $(git rev-parse origin/main) ]]; then
  echo "❌ main is not in sync with origin/main (pull or push first)"
  exit 1
fi

MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"
RELEASE_VERSION="$MAJOR.$MINOR.$PATCH"
TAG="client-cpp-v$RELEASE_VERSION"

case "$RELEASE_TYPE" in
  major) NEXT="$((MAJOR + 1)).0.0-snapshot" ;;
  minor) NEXT="$MAJOR.$((MINOR + 1)).0-snapshot" ;;
  patch) NEXT="$MAJOR.$MINOR.$((PATCH + 1))-snapshot" ;;
  *) echo "❌ Invalid release type: $RELEASE_TYPE (use major|minor|patch)"; exit 1 ;;
esac

if git rev-parse -q --verify "refs/tags/$TAG" > /dev/null; then
  echo "❌ Tag $TAG already exists"
  exit 1
fi

echo "📦 Releasing client-cpp: $VERSION → $RELEASE_VERSION"
echo "📝 Next development version: $NEXT"

# 1. Set release version
echo "$RELEASE_VERSION" > VERSION.txt
git add VERSION.txt
git commit -m "release: client-cpp $RELEASE_VERSION"

# 2. Tag with release notes (commits touching the client's sources since the previous broker tag)
PREV_TAG=$(git describe --tags --abbrev=0 --match 'client-cpp-v*' HEAD~1 2>/dev/null || echo "")
RANGE=${PREV_TAG:+$PREV_TAG..}HEAD~1
NOTES=$(git log "$RANGE" --pretty=format:"- %s" --no-merges \
  --invert-grep --grep '^chore: set next client-cpp development version' -- . ../protocol ../codec ../crypto ../ql)

git tag -a "$TAG" -m "Client release $RELEASE_VERSION

$NOTES"

# 3. Set next snapshot
echo "$NEXT" > VERSION.txt
git add VERSION.txt
git commit -m "chore: set next client-cpp development version $NEXT"

# 4. Push commits + tag
git push origin HEAD
git push origin "$TAG"

echo "✅ Released client-cpp $RELEASE_VERSION, next development version: $NEXT"
echo "🏷️  Tag: $TAG"
