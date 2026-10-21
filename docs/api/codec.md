# rune-codec

`codec/` holds three static libraries:

| Library | What it is |
|---|---|
| `rune_codec` | Schema descriptors, their binary form, the record format, validation, schema evolution and C++ generation. |
| `rune_catalog` | The catalog's event model: the `__catalog` DDL log, its fingerprints, replayer and history queries, and the `.rcat` file. |
| `rune_security` | The same for users, roles and grants: the `__security` log, permissions, replayer, queries and the `.rsec` file. |

The broker, the client and the `rune` CLI all link them, so a schema or a replayed event means the same thing everywhere.
Headers are under `codec/include/rune/{codec,catalog,security}/`.

## Types

`PrimType` ([`PrimType.h`](../../codec/include/rune/codec/PrimType.h)):

| Type | Width in the fixed block | QL name |
|---|---|---|
| `Int32`, `UInt32`, `Enum` | 4 | `int32`, `uint32` (`Enum` is not in QL) |
| `Int64`, `UInt64`, `Float64` | 8 | `int64`, `uint64`, `float64` |
| `UInt16` | 2 | `uint16` |
| `Bool` | 1 | `bool` |
| `FixedStr` | per field (`fixedLen`) | `string(N)` |
| `String`, `Bytes` | variable, after the fixed block | `string`, `bytes` |

An `Enum` field may carry a whitelist of allowed `int32` values (`enumValues`).

A **record** is `[blockLength u16][version u16][fixed block][var-len fields]`, little-endian. Fixed fields sit at fixed
offsets in the block, in declaration order. Each variable-length field follows as `[length u32][bytes]`, in declaration
order. A reader finds a field by its durable **tag** (`FieldDescriptor::id`), not by position.

## Describe, serialize, fingerprint

```cpp
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;

Descriptor d = DescriptorBuilder("player", 1)
    .add("userId", PrimType::UInt64, /*sinceVersion=*/1, /*isKey=*/true)
    .addFixedStr("name", 32)
    .add("score", PrimType::Int64)
    .build();

std::vector<uint8_t> blob = serialize(d);      // the registry's wire form (RCD2)
Descriptor back           = deserialize(blob); // RCD1 blobs are still readable
uint64_t fp               = fingerprint(d);    // FNV-1a-64, independent of declaration order
std::string text          = toText(d);         // human-readable, what `rune schema get` prints
```

`serialize()` always writes the **RCD2** form: magic `RCD2`, name, version, field count, then per field its name, durable
tag, type, `sinceVersion`, `isKey`, enum values, `fixedLen` (FixedStr) and `untilVersion`. `deserialize()` also reads
the legacy **RCD1** form. It recomputes offsets and `blockLength` and throws `std::runtime_error` on a truncated or
malformed blob. `build()` throws when two fields share a tag.

The **fingerprint** covers the schema name and, per field in tag order, its tag, type, `sinceVersion`, key flag, name,
enum values, `fixedLen` and (only for a retired field) `untilVersion`. It excludes offsets and `blockLength`, which are
derived. `fingerprintAsOf(d, v)` hashes the schema as it was at version `v`. The client binds a typed topic to the
fingerprint it was generated from.

## Validate and read

```cpp
#include <rune/codec/Validator.h>
#include <rune/codec/Record.h>

Validator v(d);
ValResult r = v.validate(recordBytes);      // r.ok, r.fieldId, r.reason ("blockLength mismatch", ...)
FieldValue f = readField(d, recordBytes, /*fieldId=*/2);   // zero-copy; f.present, f.i / f.u / f.f / f.b / f.bytes
```

`validate()` is the gate for untrusted input: it checks the header, `blockLength`, field bounds and enum whitelists.
`readField()` is the zero-copy hot path and trusts a pre-validated record, though every access is bounds-checked, so a
hostile record can only yield an absent field. A topic defined with `validate` on makes the broker run the validator on
every produced record and answer `SchemaValidationFailed` (24) on a bad one.

## Schema evolution

A topic's schema is one evolving descriptor that carries every field ever declared. Each field is live for versions in
`[sinceVersion, untilVersion)`. These are the rules, as the code enforces them (`Evolve.h`, `AlterOps.h`):

- **Add** allocates a fresh durable tag and stamps `sinceVersion = newVersion`. Old records simply lack the field.
- **Drop** retires a live field by setting `untilVersion = newVersion`. The field stays in the descriptor, so old records
  still decode it. A retired name may be reused; it gets a new tag.
- **Rename** changes the name only. Tag and type are untouched, so existing records decode byte-identically under the
  new name.
- **Set compatibility** is `Backward` (the default) or `Forward`. Under `Forward`, `Drop` is refused
  (`SchemaIncompatible`, 30), because dropping a field breaks readers written against the wider schema.
- A change is **atomic**: the first rejected operation returns its error and leaves the descriptor and the tag counter
  untouched.
- **Optimistic concurrency**: `AlterTopic.expectedVersion` must equal the topic's current version, else `SchemaConflict`
  (31).
- A new version never invalidates old data, and a client generated against an older version keeps working: the broker
  selects the schema version whose fingerprint matches the one the client was compiled with
  (`GetTopicSchema.requestedFingerprint`), and every `send` stamps that version.

```cpp
#include <rune/codec/Evolve.h>
#include <rune/codec/AlterOps.h>

uint16_t nextTag = 3;                                   // the next free durable tag
std::vector<AlterOp> ops{ {AlterOpType::Add, "guildId", "", PrimType::Int64, 0, CompatMode::Backward} };
auto next = applyOps(d, ops, nextTag, /*newVersion=*/2);   // std::expected<Descriptor, EvolveError>
Descriptor asV1 = materializeAsOf(*next, 1);            // only the fields live at version 1
```

`encodeAlterOps` / `decodeAlterOps` carry a list of operations over the wire as the `opsBlob` of `AlterTopic`.

## Generate C++ types

```cpp
#include <rune/codec/Generator.h>
std::string header = generateHeader(d, "game::gen");    // default namespace is rune::gen
```

From a shell, `rune schema generate <topic> --out gen/ --broker host:port` fetches the topic's schema and writes
`gen/<topic>.gen.h` ([admin-cli.md](admin-cli.md#rune-schema)). `rune_codec_gen <descriptor.blob> <out.h>` does the same
from a serialized descriptor file.

The generated struct is named after the schema in PascalCase and has typed members (`std::array<char, N>` for
`string(N)`, `std::string_view` for `string` and `bytes`), `encodedSize()`, `encode(uint8_t*)`, `decode(span)`, and
`kBlockLength`, `kVersion`, `kFingerprint`, `kSchemaName`, `descriptor()`. A schema with a key field also gets `key()`,
which `TypedTopic::send` routes by.

`generateHeader(evolving, version)` takes the topic's evolving descriptor (what `rune schema generate` fetches), so
the type reads records of **every** version by the record's own version: a Drop moves the fields after it, and each
field reads right or absent, never shifted. Versions up to `kVersion` need nothing; a record of a later version needs
the topic's lineage, fetched once:

```cpp
auto lin = client.lineageAndWait<game::gen::Player>("player");   // T::Lineage, cold
game::gen::Player p;
if (!p.decode(record.value, lin)) { /* a version newer than the lineage: fetch it again */ }
game::gen::Player::Decoder d(record.value, lin);                  // zero-copy; d.known()
```

`decode` returns false (every member at its default) for a version it cannot place.

## Catalog and security libraries

`rune_catalog` and `rune_security` replay an append-only event log into state. Each event carries a fingerprint over the
state it produced, each format version has frozen apply rules (so an old log replays the same forever), and a replayer
stops with a named offset on the first mismatch.

- Catalog: `CatalogEvent`, `CatalogState`, `CatalogReplayer`, `CatalogQueries` (`history`, `stateAsOf`, `diff`),
  `CatalogFile` (the `.rcat` export that `rune catalog export` writes), `TopicName` (the name rule).
- Security: `SecurityEvent`, `SecurityState`, `SecurityReplayer`, `SecurityQueries`, `Permissions` (the grant model and
  the per-session permission table), `SecurityFile` (`.rsec`).

The operator view of both is in [CATALOG.md](../../broker/docs/CATALOG.md) and
[SECURITY.md](../../broker/docs/SECURITY.md). Golden files under `codec/tests/catalog/golden` and
`codec/tests/security/golden` pin the replay results of every format version.

## Build and test

The codec CMake always builds its tests, so GTest must be found (`gtest` comes from Conan, see the
[wiki, Building](https://github.com/arcanumruna/rune/wiki/Building#conan-notes)); Google Benchmark is optional and adds `rune_codec_bench` when found.

```bash
conan install codec -of build-codec -s build_type=Debug --build=missing
cmake -S codec -B build-codec -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE=build-codec/build/Debug/generators/conan_toolchain.cmake
cmake --build build-codec
ctest --test-dir build-codec
```
