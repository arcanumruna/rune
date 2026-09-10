#include <rune/consensus/PersistentState.h>

#include <cstring>
#include <stdexcept>
#include <filesystem>
#include <system_error>

// POSIX headers for open/write/fsync/rename
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Binary layout (16 bytes, little-endian on all modern platforms):
//   [0..7]  int64_t  currentTerm
//   [8..11] int32_t  votedFor
//   [12..15] uint32_t  magic / padding = 0xCA3E4AFE
// ---------------------------------------------------------------------------
static constexpr uint32_t kMagic = 0xCA3E4AFEu;
static constexpr std::size_t kFileSize = sizeof(int64_t) + sizeof(int32_t) + sizeof(uint32_t);

PersistentState::PersistentState(std::string path)
    : path_(std::move(path))
{
    std::error_code ec;
    if (std::filesystem::exists(path_, ec) && !ec) {
        load();
    } else {
        persist(); // write defaults so the file always exists after construction
    }
}

Term   PersistentState::currentTerm() const { return currentTerm_; }
NodeId PersistentState::votedFor()    const { return votedFor_;    }

void PersistentState::setCurrentTerm(Term newTerm) {
    if (newTerm < currentTerm_) {
        throw std::invalid_argument("PersistentState::setCurrentTerm: term must not decrease");
    }
    if (newTerm == currentTerm_) return;
    currentTerm_ = newTerm;
    votedFor_    = kNoVote;   // Raft rule: new term ⇒ clear prior vote
    persist();
}

void PersistentState::setVotedFor(NodeId candidate) {
    votedFor_ = candidate;
    persist();
}

void PersistentState::update(Term newTerm, NodeId candidate) {
    if (newTerm < currentTerm_) {
        throw std::invalid_argument("PersistentState::update: term must not decrease");
    }
    currentTerm_ = newTerm;
    votedFor_    = candidate;
    persist();
}

// ---------------------------------------------------------------------------
// persist() — write-to-temp + fsync + rename (atomic durable update)
// ---------------------------------------------------------------------------
void PersistentState::persist() const {
    // Build the 16-byte payload
    std::byte buf[kFileSize];
    int64_t  term = currentTerm_;
    int32_t  vote = static_cast<int32_t>(votedFor_);
    uint32_t magic = kMagic;
    std::memcpy(buf,                          &term,  sizeof(term));
    std::memcpy(buf + sizeof(term),           &vote,  sizeof(vote));
    std::memcpy(buf + sizeof(term) + sizeof(vote), &magic, sizeof(magic));

    // Write to a sibling temp file
    std::string tmp = path_ + ".tmp";

    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(),
                                "PersistentState: open tmp " + tmp);
    }
    ssize_t written = ::write(fd, buf, kFileSize);
    if (written < 0 || static_cast<std::size_t>(written) != kFileSize) {
        ::close(fd);
        throw std::system_error(errno, std::system_category(),
                                "PersistentState: write " + tmp);
    }
    if (::fsync(fd) < 0) {
        ::close(fd);
        throw std::system_error(errno, std::system_category(),
                                "PersistentState: fsync " + tmp);
    }
    ::close(fd);

    // Atomic rename
    if (::rename(tmp.c_str(), path_.c_str()) < 0) {
        throw std::system_error(errno, std::system_category(),
                                "PersistentState: rename " + tmp + " -> " + path_);
    }

    // fsync the directory so the rename is durable
    std::string dir = std::filesystem::path(path_).parent_path().string();
    if (dir.empty()) dir = ".";
    int dfd = ::open(dir.c_str(), O_RDONLY);
    if (dfd >= 0) {
        ::fsync(dfd); // best-effort; ignore error (some FSes don't support it)
        ::close(dfd);
    }
}

// ---------------------------------------------------------------------------
// load() — read and validate
// ---------------------------------------------------------------------------
void PersistentState::load() {
    int fd = ::open(path_.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(),
                                "PersistentState: open " + path_);
    }
    std::byte buf[kFileSize] = {};
    ssize_t got = ::read(fd, buf, kFileSize);
    ::close(fd);

    if (got < 0) {
        throw std::system_error(errno, std::system_category(),
                                "PersistentState: read " + path_);
    }
    if (static_cast<std::size_t>(got) != kFileSize) {
        // Truncated / corrupt file — reset to defaults
        currentTerm_ = 0;
        votedFor_    = kNoVote;
        persist();
        return;
    }

    uint32_t magic = 0;
    std::memcpy(&magic, buf + sizeof(int64_t) + sizeof(int32_t), sizeof(magic));
    if (magic != kMagic) {
        currentTerm_ = 0;
        votedFor_    = kNoVote;
        persist();
        return;
    }

    int64_t term = 0;
    int32_t vote = 0;
    std::memcpy(&term, buf,                sizeof(term));
    std::memcpy(&vote, buf + sizeof(term), sizeof(vote));
    currentTerm_ = static_cast<Term>(term);
    votedFor_    = static_cast<NodeId>(vote);
}

} // namespace rune::consensus
