#pragma once
// FragmentAssembler.h — Rune's templated port of aeron::FragmentAssembler and
// aeron::ControlledFragmentAssembler (Aeron C++ client, FragmentAssembler.h /
// ControlledFragmentAssembler.h) — Task 11 fix 2 (review B3).
//
// Why: Aeron's assemblers store the delegate as a std::function and hand
// Subscription::poll a std::function (handler()) — two type-erased calls per
// fragment, on every shard worker's ingress/replication poll and on the runtime
// (consensus ingress, internal replication, gateway).  Here the delegate is a
// TEMPLATE argument of onFragment(), passed per poll: Subscription::poll and
// Image::poll are already templated, so the whole fragment path inlines.
//
// Reassembly logic is Aeron's, line for line:
//   - UNFRAGMENTED (BEGIN|END)   → deliver in place (zero copy);
//   - BEGIN_FRAG                 → reset this session's builder, append, record
//                                  nextTermOffset = align(offset + length + HDR, 32);
//   - middle / END               → only if offset == nextTermOffset (contiguous),
//                                  else RESET (gap → drop the partial message);
//                                  on END deliver [HDR, limit) and reset;
//   - Controlled: ABORT on the reassembled message restores the builder limit
//     (the END fragment is re-delivered by the image), else reset.
// Per-session state: aeron::BufferBuilder keyed by sessionId.  Aeron uses an
// unordered_map; here a small flat vector (linear scan — a node sees a handful
// of publisher sessions per subscription), allocation only on a NEW session.
// Unfragmented messages (the hot case) never touch the session table.
#include <BufferBuilder.h>
#include <Image.h>   // aeron::ControlledPollAction
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/DataFrameHeader.h>
#include <concurrent/logbuffer/FrameDescriptor.h>
#include <concurrent/logbuffer/Header.h>
#include <util/BitUtil.h>
#include <util/Index.h>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace rune::aeronutil {

namespace detail {

class SessionBuilders {
public:
    explicit SessionBuilders(std::size_t initialBufferLength) noexcept
        : initialBufferLength_(initialBufferLength) {}

    aeron::BufferBuilder* find(std::int32_t sessionId) noexcept {
        for (auto& s : slots_) if (s.sessionId == sessionId) return &s.builder;
        return nullptr;
    }
    aeron::BufferBuilder& get(std::int32_t sessionId) {
        if (auto* b = find(sessionId)) return *b;
        slots_.push_back(Slot{sessionId, aeron::BufferBuilder(static_cast<std::uint32_t>(initialBufferLength_))});
        return slots_.back().builder;
    }
    void erase(std::int32_t sessionId) {
        for (std::size_t i = 0; i < slots_.size(); ++i)
            if (slots_[i].sessionId == sessionId) {
                if (i + 1 != slots_.size()) slots_[i] = std::move(slots_.back());
                slots_.pop_back();
                return;
            }
    }
    [[nodiscard]] std::size_t sessionCount() const noexcept { return slots_.size(); }
    /// Erase every session whose image is gone (live(sessionId) == false).
    template <class IsLive>
    std::size_t purge(IsLive&& live) {
        std::size_t erased = 0;
        for (std::size_t i = 0; i < slots_.size();) {
            if (live(slots_[i].sessionId)) { ++i; continue; }
            if (i + 1 != slots_.size()) slots_[i] = std::move(slots_.back());
            slots_.pop_back();
            ++erased;
        }
        return erased;
    }

private:
    struct Slot {
        std::int32_t         sessionId;
        aeron::BufferBuilder builder;
        Slot(std::int32_t id, aeron::BufferBuilder&& b) noexcept : sessionId(id), builder(std::move(b)) {}
        Slot(Slot&&) noexcept = default;
        Slot& operator=(Slot&& o) noexcept {
            sessionId = o.sessionId;
            builder.~BufferBuilder();
            new (&builder) aeron::BufferBuilder(std::move(o.builder));
            return *this;
        }
    };
    std::size_t       initialBufferLength_;
    std::vector<Slot> slots_;
};

inline aeron::util::index_t nextTermOffsetAfter(aeron::util::index_t offset, aeron::util::index_t length) noexcept {
    using namespace aeron::concurrent::logbuffer;
    return aeron::util::BitUtil::align(offset + length + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT);
}

} // namespace detail

/// Templated aeron::FragmentAssembler (delegate passed per call; see header).
class FragmentAssembler {
public:
    static constexpr std::size_t kDefaultBufferLength = 4096;   // == aeron DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH

    explicit FragmentAssembler(std::size_t initialBufferLength = kDefaultBufferLength) noexcept
        : builders_(initialBufferLength) {}

    template <class Delegate>
    void onFragment(Delegate&& delegate, aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                    aeron::util::index_t length, aeron::concurrent::logbuffer::Header& header) {
        using namespace aeron::concurrent::logbuffer;
        const std::uint8_t flags = header.flags();
        if ((flags & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED) [[likely]] {
            delegate(buffer, offset, length, header);
        } else if ((flags & FrameDescriptor::BEGIN_FRAG) == FrameDescriptor::BEGIN_FRAG) {
            aeron::BufferBuilder& builder = builders_.get(header.sessionId());
            builder.reset().append(buffer, offset, length, header)
                   .nextTermOffset(detail::nextTermOffsetAfter(offset, length));
        } else if (aeron::BufferBuilder* builder = builders_.find(header.sessionId())) {
            if (offset == builder->nextTermOffset()) {
                builder->append(buffer, offset, length, header);
                if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG) {
                    // The message is builder[0, limit): Aeron 1.51's BufferBuilder starts empty (1.41.4's reserved a
                    // 32-byte frame header at the front).
                    const auto msgLength = static_cast<aeron::util::index_t>(builder->limit());
                    aeron::concurrent::AtomicBuffer msgBuffer(builder->buffer(), builder->limit());
                    delegate(msgBuffer, 0, msgLength, header);
                    builder->reset();
                } else {
                    builder->nextTermOffset(detail::nextTermOffsetAfter(offset, length));
                }
            } else {
                builder->reset();   // gap: drop the partial message
            }
        }
    }

    void deleteSessionBuffer(std::int32_t sessionId) { builders_.erase(sessionId); }
    template <class IsLive> std::size_t purgeSessions(IsLive&& live) { return builders_.purge(std::forward<IsLive>(live)); }
    [[nodiscard]] std::size_t sessionCount() const noexcept { return builders_.sessionCount(); }
    [[nodiscard]] std::size_t sessionCountForTest() const noexcept { return builders_.sessionCount(); }

private:
    detail::SessionBuilders builders_;
};

/// Templated aeron::ControlledFragmentAssembler (delegate returns ControlledPollAction).
class ControlledFragmentAssembler {
public:
    static constexpr std::size_t kDefaultBufferLength = 4096;   // == aeron DEFAULT_CONTROLLED_FRAGMENT_ASSEMBLY_BUFFER_LENGTH

    explicit ControlledFragmentAssembler(std::size_t initialBufferLength = kDefaultBufferLength) noexcept
        : builders_(initialBufferLength) {}

    template <class Delegate>
    aeron::ControlledPollAction onFragment(Delegate&& delegate, aeron::concurrent::AtomicBuffer& buffer,
                                           aeron::util::index_t offset, aeron::util::index_t length,
                                           aeron::concurrent::logbuffer::Header& header) {
        using namespace aeron::concurrent::logbuffer;
        const std::uint8_t flags = header.flags();
        aeron::ControlledPollAction action = aeron::ControlledPollAction::CONTINUE;
        if ((flags & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED) [[likely]] {
            action = delegate(buffer, offset, length, header);
        } else if ((flags & FrameDescriptor::BEGIN_FRAG) == FrameDescriptor::BEGIN_FRAG) {
            aeron::BufferBuilder& builder = builders_.get(header.sessionId());
            builder.reset().append(buffer, offset, length, header)
                   .nextTermOffset(detail::nextTermOffsetAfter(offset, length));
        } else if (aeron::BufferBuilder* builder = builders_.find(header.sessionId())) {
            const std::uint32_t limit = builder->limit();
            if (offset == builder->nextTermOffset()) {
                builder->append(buffer, offset, length, header);
                if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG) {
                    const auto msgLength = static_cast<aeron::util::index_t>(builder->limit());   // see above
                    aeron::concurrent::AtomicBuffer msgBuffer(builder->buffer(), builder->limit());
                    action = delegate(msgBuffer, 0, msgLength, header);
                    if (action == aeron::ControlledPollAction::ABORT) builder->limit(limit);   // END re-delivered
                    else builder->reset();
                } else {
                    builder->nextTermOffset(detail::nextTermOffsetAfter(offset, length));
                }
            } else {
                builder->reset();   // gap: drop the partial message
            }
        }
        return action;
    }

    void deleteSessionBuffer(std::int32_t sessionId) { builders_.erase(sessionId); }
    template <class IsLive> std::size_t purgeSessions(IsLive&& live) { return builders_.purge(std::forward<IsLive>(live)); }
    [[nodiscard]] std::size_t sessionCount() const noexcept { return builders_.sessionCount(); }
    [[nodiscard]] std::size_t sessionCountForTest() const noexcept { return builders_.sessionCount(); }

private:
    detail::SessionBuilders builders_;
};

/// Frees per-session reassembly buffers of images that went away (review m-c;
/// Aeron frees them from an unavailable-image handler, which runs on the client
/// conductor thread — a cross-thread mutation here, so instead this runs on the
/// POLLING thread, after each poll):
///   - one relaxed load per poll (Subscription::imageCount(), the same image
///     array Aeron's poll already loads);
///   - when the image count changed OR the table grew (a new session started a
///     fragmented message), drop every slot whose sessionId has no image.
/// So the table never exceeds the images live at its last growth, and returns
/// to empty once all images are gone — no timers, no cross-thread state.
class ImageLossWatch {
public:
    template <class Sub, class Asm>
    void afterPoll(const Sub& sub, Asm& assembler) {
        const int images = sub.imageCount();
        const std::size_t sessions = assembler.sessionCount();
        if (images == lastImages_ && sessions <= lastSessions_) [[likely]] return;
        lastImages_ = images;
        if (sessions != 0)
            assembler.purgeSessions([&sub](std::int32_t id) { return sub.imageBySessionId(id) != nullptr; });
        lastSessions_ = assembler.sessionCount();
    }

private:
    int         lastImages_   = -1;
    std::size_t lastSessions_ = 0;
};

} // namespace rune::aeronutil
