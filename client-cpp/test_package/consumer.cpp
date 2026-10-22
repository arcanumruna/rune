// Smallest consumer of the packaged client: public headers, one object, one link of the whole library chain.
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/Version.h>

#include <cstdio>

int main(int argc, char**) {
    const rune::client::Key key(std::uint64_t{42});
    if (argc > 1) {
        // Linked, never run: constructing a Client starts its media driver, which needs a configured data directory.
        rune::client::ClientConfig cfg;
        rune::client::Client client(cfg);
    }
    std::printf("rune-client %.*s ok, key bytes=%zu\n", static_cast<int>(rune::client::kVersion.size()),
                rune::client::kVersion.data(), key.bytes().size());
    return 0;
}
