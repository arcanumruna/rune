// src/Record.cpp
#include <rune/codec/Record.h>
#include <cstring>

namespace rune::codec {

FieldValue readField(const Descriptor& d, std::span<const uint8_t> record, uint16_t fieldId) noexcept {
    const FieldDescriptor* fd = nullptr;
    for (const auto& f : d.fields) {
        if (f.id == fieldId) { fd = &f; break; }
    }
    if (!fd) return {};

    if (record.size() < 4) return {};

    uint16_t blockLength = uint16_t(record[0]) | (uint16_t(record[1]) << 8);
    uint16_t version     = uint16_t(record[2]) | (uint16_t(record[3]) << 8);

    if (fd->sinceVersion > version) return {};

    if (!isVarLen(fd->type)) {
        size_t off = size_t{4} + fd->offset;
        size_t sz  = fieldWidth(*fd);
        if (off + sz > record.size()) return {};

        FieldValue fv;
        fv.type    = fd->type;
        fv.present = true;

        auto readU = [&](size_t n) -> uint64_t {
            uint64_t v = 0;
            for (size_t k = 0; k < n; ++k)
                v |= uint64_t(record[off + k]) << (8 * k);
            return v;
        };

        switch (fd->type) {
            case PrimType::Int32:
                fv.i = static_cast<int32_t>(static_cast<uint32_t>(readU(4)));
                break;
            case PrimType::Int64:
                fv.i = static_cast<int64_t>(readU(8));
                break;
            case PrimType::UInt16:
                fv.u = readU(2);
                break;
            case PrimType::UInt32:
            case PrimType::Enum:
                fv.u = readU(4);
                break;
            case PrimType::UInt64:
                fv.u = readU(8);
                break;
            case PrimType::Float64: {
                uint64_t raw = readU(8);
                std::memcpy(&fv.f, &raw, 8);
                break;
            }
            case PrimType::Bool:
                fv.b = record[off] != 0;
                fv.i = record[off];
                break;
            case PrimType::FixedStr:
                fv.bytes = std::string_view(reinterpret_cast<const char*>(record.data() + off), sz);
                break;
            default:
                return {};
        }
        return fv;
    } else {
        size_t pos = size_t{4} + blockLength;
        if (pos > record.size()) return {};

        // Invariant (additive-only evolution): var fields are appended in
        // non-decreasing sinceVersion order, so walking var-index order while
        // gating each by `sinceVersion <= recordVersion` matches the writer's
        // framing. Out-of-order var-field sinceVersions are unsupported.
        uint32_t varIndex = fd->offset;
        for (uint32_t j = 0; j < varIndex; ++j) {
            if (pos + 4 > record.size()) return {};
            uint32_t len = uint32_t(record[pos])
                         | (uint32_t(record[pos + 1]) << 8)
                         | (uint32_t(record[pos + 2]) << 16)
                         | (uint32_t(record[pos + 3]) << 24);
            pos += 4;
            if (pos + len > record.size()) return {};
            pos += len;
        }

        if (pos + 4 > record.size()) return {};
        uint32_t len = uint32_t(record[pos])
                     | (uint32_t(record[pos + 1]) << 8)
                     | (uint32_t(record[pos + 2]) << 16)
                     | (uint32_t(record[pos + 3]) << 24);
        pos += 4;
        if (pos + len > record.size()) return {};

        FieldValue fv;
        fv.type    = fd->type;
        fv.present = true;
        fv.bytes   = std::string_view(reinterpret_cast<const char*>(record.data() + pos), len);
        return fv;
    }
}

} // namespace rune::codec
