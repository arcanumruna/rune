// RecordDescriptor.h — the ring-buffer record framing of Aeron's OneToOne / ManyToOne ring buffers.
//
// Derived from Aeron's aeron-client C++ concurrent/ringbuffer/RecordDescriptor.h (Real Logic, Apache License 2.0;
// see NOTICE).  Aeron 1.51 ships only the C++ wrapper over the C client, which no longer carries this header, so Rune
// keeps the identical layout here.  Every ring in Rune frames records with it, so it must stay byte-identical to
// Aeron's framing:
//
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |R|                       Record Length                         |
//  +-+-------------------------------------------------------------+
//  |                              Type                             |
//  +---------------------------------------------------------------+
//  |                       Encoded Message                        ...
//
// Writing the record length (release store) signals that the record is complete.
#pragma once

#include <cstdint>
#include <string>

#include <util/Exceptions.h>
#include <util/Index.h>

namespace rune::protocol::ring::RecordDescriptor {

inline constexpr aeron::util::index_t HEADER_LENGTH = sizeof(std::int32_t) * 2;
inline constexpr aeron::util::index_t ALIGNMENT = HEADER_LENGTH;
inline constexpr std::int32_t PADDING_MSG_TYPE_ID = -1;

[[nodiscard]] constexpr aeron::util::index_t lengthOffset(aeron::util::index_t recordOffset) noexcept {
    return recordOffset;
}

[[nodiscard]] constexpr aeron::util::index_t typeOffset(aeron::util::index_t recordOffset) noexcept {
    return recordOffset + static_cast<aeron::util::index_t>(sizeof(std::int32_t));
}

[[nodiscard]] constexpr aeron::util::index_t encodedMsgOffset(aeron::util::index_t recordOffset) noexcept {
    return recordOffset + HEADER_LENGTH;
}

[[nodiscard]] constexpr std::int64_t makeHeader(std::int32_t length, std::int32_t msgTypeId) noexcept {
    return static_cast<std::int64_t>(((static_cast<std::uint64_t>(static_cast<std::uint32_t>(msgTypeId))) << 32) |
                                     static_cast<std::uint32_t>(length));
}

[[nodiscard]] constexpr std::int32_t recordLength(std::int64_t header) noexcept {
    return static_cast<std::int32_t>(header);
}

[[nodiscard]] constexpr std::int32_t messageTypeId(std::int64_t header) noexcept {
    return static_cast<std::int32_t>(header >> 32);
}

inline void checkMsgTypeId(std::int32_t msgTypeId) {
    if (msgTypeId < 1) {
        throw aeron::util::IllegalArgumentException(
            "Message type id must be greater than zero, msgTypeId=" + std::to_string(msgTypeId), SOURCEINFO);
    }
}

}  // namespace rune::protocol::ring::RecordDescriptor
