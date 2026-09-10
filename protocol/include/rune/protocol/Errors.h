#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>

namespace rune::protocol {
enum class ErrorCode : int16_t {
    None = 0,
    CorruptRecord = 2,
    OffsetOutOfRange = 1,
    InvalidRecordBatch = 3,
    IndexOffsetOverflow = 4,
};
struct RuneException : std::runtime_error {
    ErrorCode code;
    RuneException(ErrorCode c, const std::string& msg) : std::runtime_error(msg), code(c) {}
};
} // namespace rune::protocol
