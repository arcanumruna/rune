#!/usr/bin/env bash
# Usage: ./release.sh [major|minor|patch]
set -euo pipefail

RELEASE_TYPE="${1:?Usage: ./release.sh [major|minor|patch]}"
VERSION=$(< VERSION.txt)

if [[ ! $VERSION =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)-snapshot$ ]]; then
  echo "❌ VERSION.txt must be X.Y.Z-snapshot, got: $VERSION"
  exit 1
fi

MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"
RELEASE_VERSION="$MAJOR.$MINOR.$PATCH"

case "$RELEASE_TYPE" in
  major) NEXT="$((MAJOR + 1)).0.0-snapshot" ;;
  minor) NEXT="$MAJOR.$((MINOR + 1)).0-snapshot" ;;
  patch) NEXT="$MAJOR.$MINOR.$((PATCH + 1))-snapshot" ;;
  *) echo "❌ Invalid release type: $RELEASE_TYPE (use major|minor|patch)"; exit 1 ;;
esac

echo "📦 Releasing: $VERSION → $RELEASE_VERSION"
echo "📝 Next development version: $NEXT"

# 1. Set release version
echo "$RELEASE_VERSION" > VERSION.txt
git add VERSION.txt
git commit -m "release: $RELEASE_VERSION"

# 2. Tag with release notes
PREV_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [[ -n "$PREV_TAG" ]]; then
  NOTES=$(git log "$PREV_TAG"..HEAD~1 --pretty=format:"- %s" --no-merges)
else
  NOTES=$(git log HEAD~1 --pretty=format:"- %s" --no-merges)
fi

TAG_MSG="Release $RELEASE_VERSION

$NOTES"

git tag -a "v$RELEASE_VERSION" -m "$TAG_MSG"

# 3. Set next SNAPSHOT
echo "$NEXT" > VERSION.txt
git add VERSION.txt
git commit -m "chore: set next development version $NEXT"

# 4. Push commits + tag
git push origin HEAD
git push origin "v$RELEASE_VERSION"

echo "✅ Released $RELEASE_VERSION, next development version: $NEXT"
echo "🏷️  Tag: v$RELEASE_VERSION"
