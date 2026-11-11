# Release note: `create topic` forms, implied `group by`, `rune::FixedString<N>`

Three changes to Rune QL and the generated C++ types. Section 4 lists the compatibility notes; read it before upgrading
a cluster that has aggregate views.

## 1. `create topic` forms (#1008)

Topics are now defined with the same `create` verb as views, users and roles:

| New form                                  | Same as the bare form             |
|-------------------------------------------|-----------------------------------|
| `create topic NAME ...;`                  | `topic NAME ...;`                 |
| `create lease topic NAME ...;`            | `lease topic NAME ...;`           |
| `create transient topic NAME ...;`        | `transient topic NAME ...;`       |
| `create transient lease topic NAME ...;`  | `transient lease topic NAME ...;` |

The `create` forms parse to exactly the same definition as the bare forms, so nothing about the topic changes. **The bare
forms are still accepted**; existing scripts keep working. `create transient [unique] view ...` is unchanged. The docs now
show the `create` forms.

## 2. `group by` is gone from `create view` (#1009)

An aggregate view (any `count`, `sum`, `avg`, `min`, `max` or `last` item) is grouped by its key. `group by` is **refused
at parse time**: `'group by' is not part of create view: an aggregate view is grouped by its key; remove 'group by ...'`.

Migration: delete the `group by ...` clause. It had to equal the `key` list already, so the view is the same.

- Stored views, catalogs and fingerprints are unchanged: the plan encoding is byte-identical, and old plans still decode.
- `show view` and `rune ql --dry-run` no longer print a `group by` line; the key line says it.
- An old client that sends a group-by that does not match the key is now refused as `ViewFieldUnresolved` (34) instead of
  `ViewKeyInvalid` (35).
- The C++ `ViewPlan::groupBy` member is removed. A plan author sets `keyComponents` (or the single key) and the group is
  the key.

## 3. `string(N)` is `rune::FixedString<N>` (#1010)

Generated `string(N)` members, keys and filter members are `rune::FixedString<N>`
(`codec/include/rune/codec/FixedString.h`) instead of `std::array<char, N>`. It is exactly the N wire bytes, trivially
copyable and zero padded, so **layout, fingerprint and wire are unchanged**.

- A string literal assigns directly: `rec.name = "Ayla"`, or in a designated initializer. A literal longer than N is a
  compile error.
- A runtime string that is longer than N throws `std::invalid_argument` and leaves the object unchanged. `tryAssign(sv)` is
  the `noexcept` form and returns `false` instead.
- `.view()` gives the content, up to the first NUL.
- Filters: `rune::Match<rune::FixedString<N>>` replaces `rune::Match<std::array<char, N>>`.
- **Source-breaking change for flyweight callers.** The generated `Encoder` and `ClaimEncoder` `string(N)` setters took a
  `std::string_view` and silently truncated a longer value. They now take `const rune::FixedString<N>&`. A caller that
  passes a runtime `std::string` or `std::string_view` wraps it: `enc.name(rune::FixedString<16>(s))` (throws when too
  long). String literals work as before.
- Migrate `std::array<char, N>` helpers, `.fill(...)`, `memcpy` into the member and `{'a', 'b'}` initializers to
  assignment from a literal or a string.
- **Regenerate every committed generated header** (`rune schema generate`); headers generated before this release do not
  compile against the new filter types.

## 4. Compatibility

- **Brokers first for aggregate views.** An aggregate view whose key is a single plain column used to be authored as a
  plan v1; it now serializes as a **plan v2** (the group-by block is derived from the key). A broker that reads only v1
  cannot read such a plan, so upgrade every broker before a client of this release creates or alters such a view. Views
  already stored are not rewritten.
- `create topic` is parsed by the client (`rune ql`, `rune::ql::parse`), so the `create` forms need a client of this
  release; the broker sees the same request as before.
- Generated headers must be regenerated and flyweight setter calls with runtime strings wrapped (section 3).
