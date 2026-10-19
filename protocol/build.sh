#!/bin/bash
set -e
: "${CONAN_USER:?set CONAN_USER (conan package user)}"
: "${CONAN_CHANNEL:=dev}"

# cmake-PATH shim: find cmake from .conan2 if not in PATH
if ! command -v cmake >/dev/null 2>&1; then
  CMAKE_BIN=$(dirname "$(find "$HOME/.conan2" -type f -name cmake -path '*/bin/*' 2>/dev/null | head -1)")
  [ -n "$CMAKE_BIN" ] && export PATH="$CMAKE_BIN:$PATH"
fi

VERSION=$(cat VERSION.txt | tr -d '[:space:]')
PACKAGE="rune-protocol/${VERSION}@${CONAN_USER}/${CONAN_CHANNEL}"
ARCH="-s arch=x86_64"
echo "📦 $PACKAGE"
conan install . --build=missing -s build_type=Release $ARCH --format=json > conan-graph.json
cmake . -B build -DCMAKE_OSX_ARCHITECTURES=x86_64 \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_PREFIX_PATH="$PWD/build/Release/generators" -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -- VERBOSE=1
(cd build && ctest --output-on-failure)
conan create . --user="${CONAN_USER}" --channel="${CONAN_CHANNEL}" $ARCH
conan export . --user="${CONAN_USER}" --channel="${CONAN_CHANNEL}"
conan list --graph=conan-graph.json --graph-binaries=build --format=json > conan-pkglist.json
conan upload --list=conan-pkglist.json --remote=artifactory -c
conan upload "$PACKAGE" --remote=artifactory -c
echo "✅ Done."
