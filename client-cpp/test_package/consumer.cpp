// Smallest consumer of the packaged client: public headers, one object, one link of the whole library chain.
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/Version.h>

#include <cstdio>

int main() {
    const rune::client::Key key(std::uint64_t{42});
    rune::client::ClientConfig cfg;   // aeron:ipc defaults; nothing connects until connect() is called
    rune::client::Client client(cfg);
    std::printf("rune-client %.*s ok, key bytes=%zu\n", static_cast<int>(rune::client::kVersion.size()),
                rune::client::kVersion.data(), key.bytes().size());
    return 0;
}
