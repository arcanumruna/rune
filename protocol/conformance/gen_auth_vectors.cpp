// conformance/gen_auth_vectors.cpp — generator for the authentication golden vectors
// (client version 9).  Run once, commit the output; the harness only READS them.
//
//   c++ -std=c++23 -I include -I tests/protocol/client conformance/gen_auth_vectors.cpp -o /tmp/rune_gen_auth_vectors
//   /tmp/rune_gen_auth_vectors conformance/vectors
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include "AuthVectorFixtures.h"

namespace av = rune::protocol::client::authvec;

template <typename M>
static void emit(const char* dir, const char* name, const M& m) {
    std::vector<uint8_t> buf(m.encodedSize() + 16);
    const std::size_t n = m.encode(buf.data());
    if (n == 0) { std::fprintf(stderr, "%s: encode failed\n", name); std::exit(1); }
    std::string path = std::string(dir) + "/" + name;
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f || std::fwrite(buf.data(), 1, n, f) != n) { std::perror(path.c_str()); std::exit(1); }
    std::fclose(f);
    std::printf("wrote %s (%zu bytes)\n", path.c_str(), n);
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "usage: %s <vectors-dir>\n", argv[0]); return 1; }
    const char* d = argv[1];
    emit(d, "auth_client_first.bin", av::clientFirst());
    emit(d, "auth_server_first.bin", av::serverFirst());
    emit(d, "auth_client_final.bin", av::clientFinal());
    emit(d, "auth_server_final.bin", av::serverFinal());
    emit(d, "auth_server_final_failed.bin", av::serverFinalFailed());
    emit(d, "security_ddl_create_user.bin", av::createUser());
    emit(d, "security_ddl_set_password.bin", av::setPassword());
    emit(d, "security_ddl_grant.bin", av::grant());
    emit(d, "security_bootstrap.bin", av::bootstrap());
    // ConnectResponse has no encodedSize(): wrap it
    struct W { rune::protocol::client::ConnectResponse m; std::size_t encodedSize() const { return 96; }
               std::size_t encode(uint8_t* b) const { return m.encode(b); } };
    emit(d, "connect_response_v9.bin", W{av::connectResponseV9()});
    emit(d, "connect_response_v9_crypto.bin", W{av::connectResponseV9Crypto()});
    return 0;
}
