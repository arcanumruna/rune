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
ValResult s = v.validate(recordBytes, StringCheck::Utf8);   // also: valid UTF-8 and zero padding in every string
FieldValue f = readField(d, recordBytes, /*fieldId=*/2);   // zero-copy; f.present, f.i / f.u / f.f / f.b / f.bytes
```

`validate()` is the gate for untrusted input: it checks the header, `blockLength`, field bounds and enum whitelists;
with `StringCheck::Utf8` it also requires every `string` and `string(N)` to be well-formed UTF-8 and the bytes of a
`string(N)` after its first NUL to be NUL. `readField()` is the zero-copy hot path and trusts a pre-validated record, though every access is bounds-checked, so a
hostile record can only yield an absent field. The broker runs the validator on every record produced to a typed topic
and answers `SchemaValidationFailed` (24) on a bad one.

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

### Filters

For every filterable field the generated struct also gets a nested `Filter` (filter by example) used by
`SubscribeOptions::filter` and `GroupConfig::filter` ([client-cpp.md](client-cpp.md#subscription-filters)):

```cpp
using E = rune::gen::PlayerEvent;
E::Filter f{.worldId = 7u, .channel = rune::anyOf(1, 2)};   // members follow the schema's field order
rune::codec::FilterBytes b = f.encode();                    // throws std::invalid_argument on a bad value
```

- One `rune::Match<T>` member per filterable field: `Int32`, `Int64`, `UInt16`, `UInt32`, `UInt64`, `Enum`, `Bool`, and
  `std::array<char, N>` for `string(N)` up to 16 (a string literal of at most N characters is zero-padded like the
  stored field). No member exists for `string`, `bytes` or `float64`; the comment above `Filter` names them.
- A `Match` holds one value or `rune::anyOf(...)` (up to 64). Integral literals are accepted for integral fields and
  range-checked at encode time (an out-of-range value fails, it is never truncated); floats, strings and `bool` against
  integers do not compile. `encodeTo(span)` is `constexpr`; `encode()` throws.
- The bytes are canonical (terms sorted by durable tag, `In` values sorted and de-duplicated), at most 8 terms and
  `kMaxFilterBytes` (8,234) bytes; equal filters encode identically. An empty `Filter{}` encodes to nothing.
- A schema field named `Filter` makes the generator throw `std::invalid_argument` (it would collide with the nested
  type, like a field named `key`). The generated `Filter` changes no `kFingerprint`.
- `rune::codec::FilterBuilder::on(descriptor).field("name").eq(v)` / `.in({...})` / `.build()` builds the same bytes at run
  time and returns `std::expected<FilterBytes, FilterError>`; `describe(error)` names the term. Use it where only the
  descriptor is known (CLI, probes, dynamic clients).
- `rune::codec::FilterSchema` and `CompiledFilter` (`FilterSchema.h`, `FilterPredicate.h`) are what the broker evaluates
  with; they are standalone in the codec so other consumers can reuse them.

## View keys

A materialized view can be keyed by up to 128 bytes of typed components
([`ViewKeyEncoding.h`](../../codec/include/rune/codec/ViewKeyEncoding.h)). The broker folds with these functions and the client builds
keys with them, so a key built by a client is byte-identical to the broker's.

```cpp
#include <rune/codec/ViewKeyEncoding.h>
#include <rune/codec/UnicodeLower.h>
```

- **Components.** `Int32`, `Int64`, `UInt16`, `UInt32`, `UInt64`, `Enum`, `Bool` and `FixedStr` (`string(N)`); the key is their
  concatenation in declaration order, at most `kMaxViewKeyBytes` (128). `Float64`, `String` and `Bytes` are refused.
  `viewKeyComponentWidth(type, fixedLen, keyFunction)` returns the width, 0 for an invalid component.
- **Encoding** (`encodeKeyComponent`), so that a byte-wise compare orders like the values: integers big-endian, `Int32` and
  `Int64` with the sign bit flipped; `Bool` one byte, 0 or 1; `string(N)` its bytes then zero padding to `N`. `decodeKeyComponent`
  is the inverse, used to write a group key's columns into an aggregate's output row.
- **`lower()`** (key function 1, `string(N)` only): `lowerUtf8(in, out)` applies the Unicode `Simple_Lowercase_Mapping`
  to the UTF-8 in `in` up to its first `0x00` byte and zero-fills `out` to its end. It uses no locale rules and no
  normalization, so `"I"` is `"i"` in every locale and a composed and a decomposed `é` stay different keys. Invalid UTF-8, or an
  `out` too small, is `ok == false`. The table is the generated
  [`UnicodeLowerTable.inc`](../../codec/src/UnicodeLowerTable.inc), **Unicode 16.0.0** (`kUnicodeVersion`, 1,460 mappings); it
  does not change with the toolchain. `lowerCodePoint(cp)` is one code point.
- **Width of a lowered column.** A character can lower to more bytes than it has: a 2-byte character lowers to at most 3
  bytes, a 1-byte one to 1. So a `lower()` of `string(N)` occupies `maxLoweredWidth(N)` = 3 x floor(N / 2) + (N mod 2) bytes
  (`string(16)` is 24, `string(21)` is 31). The key function 2, `casefold`, is reserved and refused.
- **The pin.** A plan with a `lower()` component carries `unicodeVersion`, the packed version (`packUnicodeVersion(16, 0, 0)`);
  a plan without one carries 0. The compiler refuses a plan whose version is not `kUnicodeVersion`
  (`unicodeVersionSupported`), and a plan with the version but no `lower()` component, so a change of the table can never
  change a stored view's keys silently.

### Authored plan v2

The authored plan ([`ViewPlan.h`](../../broker/libs/view/include/rune/view/ViewPlan.h), what `CreateView` carries as `planBlob`) has
two versions. Version 1 is the original layout. Version 2 is version 1 with the version byte `2`, then:
`keyComponents` (u32 count x `{column str, keyFunction u8, outName str}`), `unique` (u8, 0 or 1), `constraints` (u32 count x
`{name str, nodes}`), `unicodeVersion` (u32) and `groupBy` (u32 count x `{column str, keyFunction u8, outName str}`), all little-endian.

- **Version rule (P7-7).** `serializeAuthored` writes version 1 unless the plan needs version 2, so every existing plan keeps
  its bytes, its fingerprint and its catalog digest. A plan needs version 2 when it has `unique`, any constraint, a
  `unicodeVersion`, a `groupBy`, or key components other than exactly one plain component named like its column (which
  rides as `key = column`). The version byte of an existing format never changes meaning.
- `deserializeAuthored` reads versions 1 and 2 (a version 1 blob has empty version 2 fields) and returns `nullopt` for a truncated
  blob, an unknown version, an out-of-range `PrimType`, `MergeRule` or `PredicateOp`, `unique` above 1 and **trailing bytes**.
  `keyFunction` is a raw byte that decodes; the compiler refuses a value it does not know.
- A version 2 plan is sent only to a cluster at the checked-writes feature level ([wire protocol](wire-protocol.md#wide-view-keys-and-scans-client-version-17)).

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
