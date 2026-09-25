# SBE schemas (rune-protocol)

`rune-internal-ring.xml` — SBE message schema for Rune's **internal, process-local**
ring-buffer messages (`WorkerOp`, `WorkerReturn`, `FlushRequest`, `FlushCompletion`).
These travel only between threads in one process over the SPSC/MPSC byte rings; some
fields (`replica`, `logPtr`) are raw in-process pointers carried as opaque `uint64`.
They MUST NOT cross a real wire or be persisted. SBE is used for the wrap-in-place
flyweight codec (encode directly into the claimed ring slot; zero-copy decode) and
offset-safety, not for cross-process schema evolution. Distinct `schemaId=7` from the
client-facing wire schema (`schemaId=120`).

## Regenerating the C++ flyweights

Generated headers live under `include/rune_protocol_internal_sbe/` (namespace
`rune::protocol::internal::sbe`) and are committed. To regenerate after editing the
schema:

```sh
JAVA=/Applications/CLion.app/Contents/jbr/Contents/Home/bin/java   # or any JDK 17+
"$JAVA" -Dsbe.target.language=cpp -Dsbe.output.dir=include \
    -jar tools/sbe-all-1.40.2.jar schema/rune-internal-ring.xml
```

The `sbe-all` fat jar (real-logic Simple Binary Encoding, `uk.co.real-logic:sbe-all`,
now the aeron-io project) is vendored under `tools/` for reproducible, offline codegen.
