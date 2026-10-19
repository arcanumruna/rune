# rune-crypto

`rune-crypto` is a C++23 static library providing CPU-capability-based cipher selection and (in later tasks) AEAD encryption and TLS session wiring for Rune's client↔broker transport; it depends only on OpenSSL 3.x and is built and consumed via Conan + CMake like its sibling Rune libraries.
