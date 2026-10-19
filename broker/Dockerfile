FROM rune-cpp-build:latest AS build
WORKDIR /app
COPY . .
ARG ARTIFACTORY_HOST
ARG CONAN_PASSWORD
RUN conan install . --build=missing -s build_type=Release -s arch=x86_64
RUN cmake . -B build -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
      -DCMAKE_PREFIX_PATH="/app/build/Release/generators" -DCMAKE_BUILD_TYPE=Release
RUN cmake --build build -j

FROM debian:bookworm-slim
COPY --from=build /app/build/rune_broker /usr/local/bin/rune_broker
ENTRYPOINT ["/usr/local/bin/rune_broker"]
