#include <rune/node/PemFile.h>

#include <fstream>
#include <sstream>

namespace rune::node {

bool loadPemFile(const std::string& path, std::string& out) noexcept {
    out.clear();
    if (path.empty()) return false;
    try {
        std::ifstream f(path, std::ios::binary);
        if (!f) return false;
        std::ostringstream ss;
        ss << f.rdbuf();
        out = ss.str();
    } catch (...) {
        out.clear();
        return false;
    }
    return !out.empty();
}

} // namespace rune::node
