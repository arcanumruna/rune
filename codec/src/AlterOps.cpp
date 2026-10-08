// src/AlterOps.cpp
#include <rune/codec/AlterOps.h>

namespace rune::codec {

// ─── Encode helpers ───────────────────────────────────────────────────────────

static void push_u8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

static void push_u16le(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xffu));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xffu));
}

static void push_str(std::vector<uint8_t>& buf, const std::string& s) {
    // Callers never pass strings anywhere near 64KiB for ALTER op names;
    // truncate the length rather than throw, since this is a fire-and-forget
    // encode with no error channel.
    uint16_t len = static_cast<uint16_t>(s.size() <= 0xffffu ? s.size() : 0xffffu);
    push_u16le(buf, len);
    for (uint16_t i = 0; i < len; ++i) {
        buf.push_back(static_cast<uint8_t>(s[i]));
    }
}

// ─── Decode helpers (bounds-checked, no exceptions) ──────────────────────────
// Each returns false (leaving `pos` unspecified) on truncation, so callers
// can bail out to std::nullopt immediately.

static bool read_u8(std::span<const uint8_t> blob, size_t& pos, uint8_t& out) {
    if (pos + 1 > blob.size()) return false;
    out = blob[pos];
    pos += 1;
    return true;
}

static bool read_u16le(std::span<const uint8_t> blob, size_t& pos, uint16_t& out) {
    if (pos + 2 > blob.size()) return false;
    out = static_cast<uint16_t>(blob[pos])
        | static_cast<uint16_t>(static_cast<uint16_t>(blob[pos + 1]) << 8);
    pos += 2;
    return true;
}

static bool read_str(std::span<const uint8_t> blob, size_t& pos, std::string& out) {
    uint16_t len;
    if (!read_u16le(blob, pos, len)) return false;
    if (pos + len > blob.size()) return false;
    out.assign(reinterpret_cast<const char*>(blob.data() + pos), len);
    pos += len;
    return true;
}

// ─── Public API ───────────────────────────────────────────────────────────────

std::vector<uint8_t> encodeAlterOps(const std::vector<AlterOp>& ops) {
    std::vector<uint8_t> buf;
    buf.reserve(2 + ops.size() * 16);

    push_u16le(buf, static_cast<uint16_t>(ops.size() <= 0xffffu ? ops.size() : 0xffffu));

    for (const auto& op : ops) {
        push_u8(buf, static_cast<uint8_t>(op.type));
        switch (op.type) {
            case AlterOpType::Add:
                push_str(buf, op.name);
                push_u8(buf, static_cast<uint8_t>(op.primType));
                push_u16le(buf, op.fixedLen);
                break;
            case AlterOpType::Drop:
                push_str(buf, op.name);
                break;
            case AlterOpType::Rename:
                push_str(buf, op.name);
                push_str(buf, op.newName);
                break;
            case AlterOpType::SetCompatibility:
                push_u8(buf, static_cast<uint8_t>(op.mode));
                break;
        }
    }

    return buf;
}

std::optional<std::vector<AlterOp>> decodeAlterOps(std::span<const uint8_t> blob) {
    size_t pos = 0;

    uint16_t opsCount;
    if (!read_u16le(blob, pos, opsCount)) return std::nullopt;

    std::vector<AlterOp> ops;
    ops.reserve(opsCount);

    constexpr uint8_t kMaxPrimType = static_cast<uint8_t>(PrimType::FixedStr);

    for (uint16_t i = 0; i < opsCount; ++i) {
        uint8_t opTypeByte;
        if (!read_u8(blob, pos, opTypeByte)) return std::nullopt;
        if (opTypeByte < static_cast<uint8_t>(AlterOpType::Add)
            || opTypeByte > static_cast<uint8_t>(AlterOpType::SetCompatibility)) {
            return std::nullopt;
        }
        AlterOpType opType = static_cast<AlterOpType>(opTypeByte);

        AlterOp op{};
        op.type = opType;

        switch (opType) {
            case AlterOpType::Add: {
                if (!read_str(blob, pos, op.name)) return std::nullopt;
                uint8_t primByte;
                if (!read_u8(blob, pos, primByte)) return std::nullopt;
                if (primByte > kMaxPrimType) return std::nullopt;
                op.primType = static_cast<PrimType>(primByte);
                if (!read_u16le(blob, pos, op.fixedLen)) return std::nullopt;
                break;
            }
            case AlterOpType::Drop: {
                if (!read_str(blob, pos, op.name)) return std::nullopt;
                break;
            }
            case AlterOpType::Rename: {
                if (!read_str(blob, pos, op.name)) return std::nullopt;
                if (!read_str(blob, pos, op.newName)) return std::nullopt;
                break;
            }
            case AlterOpType::SetCompatibility: {
                uint8_t modeByte;
                if (!read_u8(blob, pos, modeByte)) return std::nullopt;
                if (modeByte != static_cast<uint8_t>(CompatMode::Backward)
                    && modeByte != static_cast<uint8_t>(CompatMode::Forward)) {
                    return std::nullopt;
                }
                op.mode = static_cast<CompatMode>(modeByte);
                break;
            }
        }

        ops.push_back(std::move(op));
    }

    return ops;
}

} // namespace rune::codec
