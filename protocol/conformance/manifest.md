# Rune Client Protocol — Conformance Vector Manifest

**Schema:** `kSchemaId = 120`, `kProtocolVersion = 1`  
**Common IDs (all vectors):** `sessionId = 0x1122334455667788`, `correlationId = 0x0A0B0C0D0E0F1011`

All multi-byte integers are **little-endian**.  
Header layout (24 bytes): `blockLength[2] | templateId[2] | schemaId[2] | version[2] | sessionId[8] | correlationId[8]`

---

## 1. `connect_request.bin` — 67 bytes

**Message:** `ConnectRequest` (templateId = 1)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 8                        | 0x0008                          |
| templateId      | 1                        | 0x0001                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| egressStreamId  | 1001                     | 0x000003E9                      |
| clientVersion   | 1                        | 0x00000001                      |
| egressChannel   | "aeron:udp?endpoint=10.0.0.5:40456" | len=33 (0x0021), 33 bytes UTF-8 |

**Byte layout:**
```
Offset  Hex                                        Notes
00      08 00                                      blockLength = 8 (LE)
02      01 00                                      templateId = 1 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      E9 03 00 00                                egressStreamId = 1001 (LE)
28      01 00 00 00                                clientVersion = 1 (LE)
32      21 00                                      egressChannel var-len = 33 (LE)
34      61 65 72 6F 6E 3A 75 64 70 3F 65 6E 64 70  "aeron:udp?endp"
        6F 69 6E 74 3D 31 30 2E 30 2E 30 2E 35 3A  "oint=10.0.0.5:"
        34 30 34 35 36                              "40456"
```

---

## 2. `connect_request_empty_channel.bin` — 34 bytes

**Message:** `ConnectRequest` (templateId = 1)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 8                        | 0x0008                          |
| templateId      | 1                        | 0x0001                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| egressStreamId  | 1001                     | 0x000003E9                      |
| clientVersion   | 1                        | 0x00000001                      |
| egressChannel   | ""                       | len=0 (0x0000)                  |

**Byte layout:**
```
Offset  Hex                                        Notes
00      08 00                                      blockLength = 8 (LE)
02      01 00                                      templateId = 1 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      E9 03 00 00                                egressStreamId = 1001 (LE)
28      01 00 00 00                                clientVersion = 1 (LE)
32      00 00                                      egressChannel var-len = 0 (LE)
```

---

## 3. `connect_response.bin` — 38 bytes

**Message:** `ConnectResponse` (templateId = 2)

| Field              | Value (decimal)          | Value (hex)           |
|--------------------|--------------------------|-----------------------|
| blockLength        | 14                       | 0x000E                |
| templateId         | 2                        | 0x0002                |
| schemaId           | 120                      | 0x0078                |
| version            | 1                        | 0x0001                |
| sessionId          | 1234605616436508552      | 0x1122334455667788    |
| correlationId      | 723685415333974033       | 0x0A0B0C0D0E0F1011    |
| code               | 0 (OK)                   | 0x0000                |
| leaderMemberId     | 2                        | 0x00000002            |
| assignedSessionId  | 3735928559               | 0x00000000DEADBEEF    |

**Byte layout:**
```
Offset  Hex                                        Notes
00      0E 00                                      blockLength = 14 (LE)
02      02 00                                      templateId = 2 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      00 00                                      code = 0 / OK (LE)
26      02 00 00 00                                leaderMemberId = 2 (LE)
30      EF BE AD DE 00 00 00 00                    assignedSessionId = 0xDEADBEEF (LE)
```

---

## 4. `connect_response_redirect.bin` — 38 bytes

**Message:** `ConnectResponse` (templateId = 2)

| Field              | Value (decimal)          | Value (hex)           |
|--------------------|--------------------------|-----------------------|
| blockLength        | 14                       | 0x000E                |
| templateId         | 2                        | 0x0002                |
| schemaId           | 120                      | 0x0078                |
| version            | 1                        | 0x0001                |
| sessionId          | 1234605616436508552      | 0x1122334455667788    |
| correlationId      | 723685415333974033       | 0x0A0B0C0D0E0F1011    |
| code               | 3 (Redirect)             | 0x0003                |
| leaderMemberId     | 1                        | 0x00000001            |
| assignedSessionId  | 0                        | 0x0000000000000000    |

**Byte layout:**
```
Offset  Hex                                        Notes
00      0E 00                                      blockLength = 14 (LE)
02      02 00                                      templateId = 2 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      03 00                                      code = 3 / Redirect (LE)
26      01 00 00 00                                leaderMemberId = 1 (LE)
30      00 00 00 00 00 00 00 00                    assignedSessionId = 0 (LE)
```

---

## 5. `keepalive.bin` — 24 bytes

**Message:** `Keepalive` (templateId = 3, BODY_FIXED = 0 — header only)

| Field           | Value (decimal)          | Value (hex)           |
|-----------------|--------------------------|-----------------------|
| blockLength     | 0                        | 0x0000                |
| templateId      | 3                        | 0x0003                |
| schemaId        | 120                      | 0x0078                |
| version         | 1                        | 0x0001                |
| sessionId       | 1234605616436508552      | 0x1122334455667788    |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011    |

**Byte layout:**
```
Offset  Hex                                        Notes
00      00 00                                      blockLength = 0 (LE)
02      03 00                                      templateId = 3 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
```

---

## 6. `disconnect.bin` — 26 bytes

**Message:** `Disconnect` (templateId = 4)

| Field           | Value (decimal)          | Value (hex)           |
|-----------------|--------------------------|-----------------------|
| blockLength     | 2                        | 0x0002                |
| templateId      | 4                        | 0x0004                |
| schemaId        | 120                      | 0x0078                |
| version         | 1                        | 0x0001                |
| sessionId       | 1234605616436508552      | 0x1122334455667788    |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011    |
| reason          | -1                       | 0xFFFF                |

**Byte layout:**
```
Offset  Hex                                        Notes
00      02 00                                      blockLength = 2 (LE)
02      04 00                                      templateId = 4 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      FF FF                                      reason = -1 (LE int16)
```

---

## 7. `error_response.bin` — 53 bytes

**Message:** `ErrorResponse` (templateId = 5)

| Field           | Value (decimal)          | Value (hex)           |
|-----------------|--------------------------|-----------------------|
| blockLength     | 2                        | 0x0002                |
| templateId      | 5                        | 0x0005                |
| schemaId        | 120                      | 0x0078                |
| version         | 1                        | 0x0001                |
| sessionId       | 1234605616436508552      | 0x1122334455667788    |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011    |
| code            | 4 (SessionExpired)       | 0x0004                |
| message         | "session 0x1122... expired" | len=25 (0x0019), 25 UTF-8 bytes |

**Byte layout:**
```
Offset  Hex                                        Notes
00      02 00                                      blockLength = 2 (LE)
02      05 00                                      templateId = 5 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      04 00                                      code = 4 / SessionExpired (LE)
26      19 00                                      message var-len = 25 (LE)
28      73 65 73 73 69 6F 6E 20 30 78 31 31 32 32  "session 0x1122"
        2E 2E 2E 20 65 78 70 69 72 65 64           "... expired"
```

---

## 8. `error_response_empty.bin` — 28 bytes

**Message:** `ErrorResponse` (templateId = 5)

| Field           | Value (decimal)          | Value (hex)           |
|-----------------|--------------------------|-----------------------|
| blockLength     | 2                        | 0x0002                |
| templateId      | 5                        | 0x0005                |
| schemaId        | 120                      | 0x0078                |
| version         | 1                        | 0x0001                |
| sessionId       | 1234605616436508552      | 0x1122334455667788    |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011    |
| code            | 6 (Internal)             | 0x0006                |
| message         | ""                       | len=0 (0x0000)        |

**Byte layout:**
```
Offset  Hex                                        Notes
00      02 00                                      blockLength = 2 (LE)
02      05 00                                      templateId = 5 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      06 00                                      code = 6 / Internal (LE)
26      00 00                                      message var-len = 0 (LE)
```

---

## 9. `ping.bin` — 32 bytes

**Message:** `Ping` (templateId = 6, BODY_FIXED = 0)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 0                        | 0x0000                          |
| templateId      | 6                        | 0x0006                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| echo            | binary 6 bytes           | len=6 (0x0006); bytes: 00 01 02 FF FE 00 |

Note: echo contains embedded NUL bytes and 0xFF — intentionally non-UTF-8.

**Byte layout:**
```
Offset  Hex                                        Notes
00      00 00                                      blockLength = 0 (LE)
02      06 00                                      templateId = 6 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      06 00                                      echo var-len = 6 (LE)
26      00 01 02 FF FE 00                          echo bytes (binary)
```

---

## 10. `pong.bin` — 32 bytes

**Message:** `Pong` (templateId = 7, BODY_FIXED = 0)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 0                        | 0x0000                          |
| templateId      | 7                        | 0x0007                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| echo            | binary 6 bytes           | len=6 (0x0006); bytes: 00 01 02 FF FE 00 |

Identical layout to `ping.bin`, only `templateId` differs (7 vs 6).

**Byte layout:**
```
Offset  Hex                                        Notes
00      00 00                                      blockLength = 0 (LE)
02      07 00                                      templateId = 7 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      06 00                                      echo var-len = 6 (LE)
26      00 01 02 FF FE 00                          echo bytes (binary)
```

---

---

## 11. `produce_explicit_partition.bin` — 60 bytes

**Message:** `Produce` (templateId = 16)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 24                       | 0x0018                          |
| templateId      | 16                       | 0x0010                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| topicId         | 1001                     | 0x000003E9                      |
| partition       | 7                        | 0x00000007                      |
| partitionKnown  | 1                        | 0x01                            |
| acks            | 0                        | 0x00                            |
| producerId      | -1                       | 0xFFFFFFFFFFFFFFFF (sentinel)   |
| producerEpoch   | 0                        | 0x0000                          |
| baseSequence    | -1                       | 0xFFFFFFFF (sentinel)           |
| key             | (empty)                  | len=0 (0x0000)                  |
| recordBatch     | 8 opaque bytes           | len=8 (0x0008); DE AD BE EF 00 FF 01 02 |

**Byte layout:**
```
Offset  Hex                                        Notes
00      18 00                                      blockLength = 24 (LE)
02      10 00                                      templateId = 16 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      E9 03 00 00                                topicId = 1001 (LE)
28      07 00 00 00                                partition = 7 (LE)
32      01                                         partitionKnown = 1
33      00                                         acks = 0
34      FF FF FF FF FF FF FF FF                    producerId = -1 (sentinel, LE)
42      00 00                                      producerEpoch = 0 (LE)
44      FF FF FF FF                                baseSequence = -1 (sentinel, LE)
48      00 00                                      key var-len = 0 (LE)
50      08 00                                      recordBatch var-len = 8 (LE)
52      DE AD BE EF 00 FF 01 02                    recordBatch bytes (opaque)
```

---

## 12. `produce_key_only.bin` — 63 bytes

**Message:** `Produce` (templateId = 16)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 24                       | 0x0018                          |
| templateId      | 16                       | 0x0010                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| topicId         | 2002                     | 0x000007D2                      |
| partition       | 0                        | 0x00000000 (ignored; partitionKnown=0) |
| partitionKnown  | 0                        | 0x00                            |
| acks            | 0                        | 0x00                            |
| producerId      | -1                       | 0xFFFFFFFFFFFFFFFF (sentinel)   |
| producerEpoch   | 0                        | 0x0000                          |
| baseSequence    | -1                       | 0xFFFFFFFF (sentinel)           |
| key             | "ABC" (3 bytes)          | len=3 (0x0003); 41 42 43        |
| recordBatch     | 8 opaque bytes           | len=8 (0x0008); DE AD BE EF 00 FF 01 02 |

**Byte layout:**
```
Offset  Hex                                        Notes
00      18 00                                      blockLength = 24 (LE)
02      10 00                                      templateId = 16 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      D2 07 00 00                                topicId = 2002 (LE)
28      00 00 00 00                                partition = 0 (LE, ignored)
32      00                                         partitionKnown = 0
33      00                                         acks = 0
34      FF FF FF FF FF FF FF FF                    producerId = -1 (sentinel, LE)
42      00 00                                      producerEpoch = 0 (LE)
44      FF FF FF FF                                baseSequence = -1 (sentinel, LE)
48      03 00                                      key var-len = 3 (LE)
50      41 42 43                                   key bytes = "ABC"
53      08 00                                      recordBatch var-len = 8 (LE)
55      DE AD BE EF 00 FF 01 02                    recordBatch bytes (opaque)
```

---

## 13. `produce_response.bin` — 46 bytes (D6)

D6 appended `logStartOffset : int64` (here -1 = not carried) at offset 38 and
raised blockLength to 22.  The pre-D6 38-byte form below is kept verbatim as
`produce_response_v0.bin` (compat vector: a new decoder reads it with
logStartOffset = -1).  `produce_response_unknown_pid.bin` is the same message
with errorCode = 28 (UnknownProducerId), baseOffset = 0, logStartOffset = 500.

### 13 (pre-D6 layout, = `produce_response_v0.bin`) — 38 bytes

**Message:** `ProduceResponse` (templateId = 17)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 14                       | 0x000E                          |
| templateId      | 17                       | 0x0011                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| errorCode       | 0 (OK)                   | 0x0000                          |
| partition       | 7                        | 0x00000007                      |
| baseOffset      | 123456                   | 0x000000000001E240              |

**Byte layout:**
```
Offset  Hex                                        Notes
00      0E 00                                      blockLength = 14 (LE)
02      11 00                                      templateId = 17 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      00 00                                      errorCode = 0 / OK (LE)
26      07 00 00 00                                partition = 7 (LE)
30      40 E2 01 00 00 00 00 00                    baseOffset = 123456 (LE)
```

---

## 14. `subscribe_explicit.bin` — 44 bytes

**Message:** `Subscribe` (templateId = 18)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 18                       | 0x0012                          |
| templateId      | 18                       | 0x0012                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| topicId         | 3001                     | 0x000000BB9                     |
| partition       | 5                        | 0x00000005                      |
| partitionKnown  | 1                        | 0x01                            |
| fromOffset      | 9999                     | 0x000000000000270F              |
| isolation       | 1 (read_committed)       | 0x01                            |
| key             | (empty)                  | len=0 (0x0000)                  |

**Byte layout:**
```
Offset  Hex                                        Notes
00      12 00                                      blockLength = 18 (LE)
02      12 00                                      templateId = 18 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      B9 0B 00 00                                topicId = 3001 (LE)
28      05 00 00 00                                partition = 5 (LE)
32      01                                         partitionKnown = 1
33      0F 27 00 00 00 00 00 00                    fromOffset = 9999 (LE)
41      01                                         isolation = 1 (read_committed)
42      00 00                                      key var-len = 0 (LE)
```

---

## 15. `subscribe_key_only.bin` — 47 bytes

**Message:** `Subscribe` (templateId = 18)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 18                       | 0x0012                          |
| templateId      | 18                       | 0x0012                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| topicId         | 4004                     | 0x00000FA4                      |
| partition       | 0                        | 0x00000000 (ignored; partitionKnown=0) |
| partitionKnown  | 0                        | 0x00                            |
| fromOffset      | 0                        | 0x0000000000000000              |
| isolation       | 0 (read_uncommitted)     | 0x00                            |
| key             | "ABC" (3 bytes)          | len=3 (0x0003); 41 42 43        |

**Byte layout:**
```
Offset  Hex                                        Notes
00      12 00                                      blockLength = 18 (LE)
02      12 00                                      templateId = 18 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      A4 0F 00 00                                topicId = 4004 (LE)
28      00 00 00 00                                partition = 0 (LE, ignored)
32      00                                         partitionKnown = 0
33      00 00 00 00 00 00 00 00                    fromOffset = 0 (LE)
41      00                                         isolation = 0 (read_uncommitted)
42      03 00                                      key var-len = 3 (LE)
44      41 42 43                                   key bytes = "ABC"
```

---

## 16. `records_push.bin` — 58 bytes

**Message:** `Records` (templateId = 20)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 24                       | 0x0018                          |
| templateId      | 20                       | 0x0014                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| subscriptionId  | 72623859790382856       | 0x0102030405060708              |
| topicId         | 3001                     | 0x00000BB9                      |
| partition       | 5                        | 0x00000005                      |
| baseOffset      | 100000                   | 0x00000000000186A0              |
| recordBatch     | 8 opaque bytes           | len=8 (0x0008); DE AD BE EF 00 FF 01 02 |

**Byte layout:**
```
Offset  Hex                                        Notes
00      18 00                                      blockLength = 24 (LE)
02      14 00                                      templateId = 20 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      08 07 06 05 04 03 02 01                    subscriptionId (LE)
32      B9 0B 00 00                                topicId = 3001 (LE)
36      05 00 00 00                                partition = 5 (LE)
40      A0 86 01 00 00 00 00 00                    baseOffset = 100000 (LE)
48      08 00                                      recordBatch var-len = 8 (LE)
50      DE AD BE EF 00 FF 01 02                    recordBatch bytes (opaque)
```

---

## 17. `unsubscribe.bin` — 32 bytes

**Message:** `Unsubscribe` (templateId = 21)

| Field           | Value (decimal)          | Value (hex)                     |
|-----------------|--------------------------|---------------------------------|
| blockLength     | 8                        | 0x0008                          |
| templateId      | 21                       | 0x0015                          |
| schemaId        | 120                      | 0x0078                          |
| version         | 1                        | 0x0001                          |
| sessionId       | 1234605616436508552      | 0x1122334455667788              |
| correlationId   | 723685415333974033       | 0x0A0B0C0D0E0F1011              |
| subscriptionId  | 72623859790382856       | 0x0102030405060708              |

**Byte layout:**
```
Offset  Hex                                        Notes
00      08 00                                      blockLength = 8 (LE)
02      15 00                                      templateId = 21 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      08 07 06 05 04 03 02 01                    subscriptionId (LE)
```

---

## Session Flow

### Control session flow

The `session_flow` sequence models a real client session round-trip at the wire level.

| Order | File                   | templateId | Message         |
|-------|------------------------|------------|-----------------|
| 1     | connect_request.bin    | 1          | ConnectRequest  |
| 2     | connect_response.bin   | 2          | ConnectResponse |
| 3     | keepalive.bin          | 3          | Keepalive       |
| 4     | ping.bin               | 6          | Ping            |
| 5     | pong.bin               | 7          | Pong            |
| 6     | disconnect.bin         | 4          | Disconnect      |

**Expected templateId sequence:** `[1, 2, 3, 6, 7, 4]`

### Produce→Records session flow (PB-3)

Models a produce + subscribe round-trip: client produces a batch, receives a
response with baseOffset, subscribes at that offset, receives records, then
unsubscribes.

| Order | File                          | templateId | Message         | Direction        |
|-------|-------------------------------|------------|-----------------|------------------|
| 1     | produce_explicit_partition.bin| 16         | Produce         | client → broker  |
| 2     | produce_response.bin          | 17         | ProduceResponse | broker → client  |
| 3     | subscribe_explicit.bin        | 18         | Subscribe       | client → broker  |
| 4     | records_push.bin              | 20         | Records         | broker → client  |
| 5     | unsubscribe.bin               | 21         | Unsubscribe     | client → broker  |

**Expected templateId sequence:** `[16, 17, 18, 20, 21]`

Note: SubscribeResponse (templateId=19) is a known dispatch template but does not
have a standalone golden vector in this set; it is exercised via the dispatch harness.

---

## ErrorCode enum values

| Name                | Value |
|---------------------|-------|
| OK                  | 0     |
| ProtocolVersion     | 1     |
| NotLeader           | 2     |
| Redirect            | 3     |
| SessionExpired      | 4     |
| UnknownSession      | 5     |
| Internal            | 6     |
| UnknownTopic        | 7     |
| UnknownPartition    | 8     |
| NotPartitionLeader  | 9     |

---

## TemplateId enum values

| Name            | Value | Range           |
|-----------------|-------|-----------------|
| Connect         | 1     | control (1-15)  |
| ConnectResponse | 2     | control (1-15)  |
| Keepalive       | 3     | control (1-15)  |
| Disconnect      | 4     | control (1-15)  |
| ErrorResponse   | 5     | control (1-15)  |
| Ping            | 6     | control (1-15)  |
| Pong            | 7     | control (1-15)  |
| Produce           | 16    | produce/consume (16-31) |
| ProduceResponse   | 17    | produce/consume (16-31) |
| Subscribe         | 18    | produce/consume (16-31) |
| SubscribeResponse | 19    | produce/consume (16-31) |
| Records           | 20    | produce/consume (16-31) |
| Unsubscribe       | 21    | produce/consume (16-31) |
| JoinGroup         | 32    | groups/txn (32-63) |
| JoinGroupResponse | 33    | groups/txn (32-63) |
| SyncGroup         | 34    | groups/txn (32-63) |
| SyncGroupResponse | 35    | groups/txn (32-63) |
| Heartbeat         | 36    | groups/txn (32-63) |
| HeartbeatResponse | 37    | groups/txn (32-63) |
| LeaveGroup        | 38    | groups/txn (32-63) |
| LeaveGroupResponse| 39    | groups/txn (32-63) |
| OffsetCommit      | 40    | groups/txn (32-63) |
| OffsetCommitResponse | 41 | groups/txn (32-63) |
| OffsetFetch       | 42    | groups/txn (32-63) |
| OffsetFetchResponse | 43 | groups/txn (32-63) |
| Assign            | 44    | groups/txn (32-63) |

---

## CG-1 Golden vectors (consumer-group protocol, templateIds 32–44)

All vectors use the same session/correlation IDs as the rest of the manifest.
`groupId = "grp2"` (4 bytes), `memberId = "mbrX"` (4 bytes).

### 18. `cg1_join_group.bin` — 50 bytes

**Message:** `JoinGroup` (templateId = 32)

| Field               | Value             | Hex             |
|---------------------|-------------------|-----------------|
| blockLength         | 4                 | 0x0004          |
| templateId          | 32                | 0x0020          |
| schemaId            | 120               | 0x0078          |
| version             | 1                 | 0x0001          |
| sessionId           | 0x1122334455667788 | see header     |
| correlationId       | 0x0A0B0C0D0E0F1011 | see header     |
| sessionTimeoutMs    | 30000             | 0x00007530      |
| groupId             | "grp2"            | len=4; 67 72 70 32 |
| memberId            | "mbrX"            | len=4; 6D 62 72 58 |
| subscribedTopicIds  | [10, 20]          | count=2; 0A000000 14000000 |

**Byte layout:**
```
Offset  Hex                                        Notes
00      04 00                                      blockLength = 4 (LE)
02      20 00                                      templateId = 32 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      30 75 00 00                                sessionTimeoutMs = 30000 (LE)
28      04 00                                      groupId var-len = 4 (LE)
30      67 72 70 32                                "grp2"
34      04 00                                      memberId var-len = 4 (LE)
36      6D 62 72 58                                "mbrX"
40      02 00                                      topicIds count = 2 (LE)
42      0A 00 00 00                                topicId[0] = 10 (LE)
46      14 00 00 00                                topicId[1] = 20 (LE)
```

---

### 19. `cg1_sync_group_response.bin` — 44 bytes

**Message:** `SyncGroupResponse` (templateId = 35)

| Field         | Value               | Hex             |
|---------------|---------------------|-----------------|
| blockLength   | 2                   | 0x0002          |
| templateId    | 35                  | 0x0023          |
| schemaId      | 120                 | 0x0078          |
| version       | 1                   | 0x0001          |
| sessionId     | 0x1122334455667788  | see header      |
| correlationId | 0x0A0B0C0D0E0F1011  | see header      |
| errorCode     | 0 (OK)              | 0x0000          |
| assignment    | [(10,0), (20,3)]    | count=2; stride 8 bytes each |

**Byte layout:**
```
Offset  Hex                                        Notes
00      02 00                                      blockLength = 2 (LE)
02      23 00                                      templateId = 35 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      00 00                                      errorCode = 0 / OK (LE)
26      02 00                                      assignment count = 2 (LE)
28      0A 00 00 00                                assignment[0].topicId = 10 (LE)
32      00 00 00 00                                assignment[0].partition = 0 (LE)
36      14 00 00 00                                assignment[1].topicId = 20 (LE)
40      03 00 00 00                                assignment[1].partition = 3 (LE)
```

---

### 20. `cg1_offset_commit.bin` — 64 bytes

**Message:** `OffsetCommit` (templateId = 40)

Encodes 1 entry with 4-byte metadata `{DE AD BE EF}`.

| Field         | Value               | Hex             |
|---------------|---------------------|-----------------|
| blockLength   | 4                   | 0x0004          |
| templateId    | 40                  | 0x0028          |
| schemaId      | 120                 | 0x0078          |
| version       | 1                   | 0x0001          |
| sessionId     | 0x1122334455667788  | see header      |
| correlationId | 0x0A0B0C0D0E0F1011  | see header      |
| generationId  | 5                   | 0x00000005      |
| groupId       | "grp2"              | len=4           |
| memberId      | "mbrX"              | len=4           |
| entries count | 1                   | 0x0001          |
| entry.topicId | 10                  | 0x0000000A      |
| entry.partition | 0                 | 0x00000000      |
| entry.offset  | 1000                | 0x00000000000003E8 |
| entry.metadata| {DE,AD,BE,EF}       | len=4           |

**Byte layout:**
```
Offset  Hex                                        Notes
00      04 00                                      blockLength = 4 (LE)
02      28 00                                      templateId = 40 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      05 00 00 00                                generationId = 5 (LE)
28      04 00                                      groupId var-len = 4 (LE)
30      67 72 70 32                                "grp2"
34      04 00                                      memberId var-len = 4 (LE)
36      6D 62 72 58                                "mbrX"
40      01 00                                      entries count = 1 (LE)
42      0A 00 00 00                                entry.topicId = 10 (LE)
46      00 00 00 00                                entry.partition = 0 (LE)
50      E8 03 00 00 00 00 00 00                    entry.offset = 1000 (LE)
58      04 00                                      entry.meta var-len = 4 (LE)
60      DE AD BE EF                                entry.metadata bytes
```

---

### 21. `cg1_assign_revoke.bin` — 30 bytes

**Message:** `Assign` (templateId = 44)

Empty assignment array signals a full revoke (consumer loses all partitions).

| Field         | Value               | Hex             |
|---------------|---------------------|-----------------|
| blockLength   | 4                   | 0x0004          |
| templateId    | 44                  | 0x002C          |
| schemaId      | 120                 | 0x0078          |
| version       | 1                   | 0x0001          |
| sessionId     | 0x1122334455667788  | see header      |
| correlationId | 0x0A0B0C0D0E0F1011  | see header      |
| generationId  | 7                   | 0x00000007      |
| assignment    | [] (empty = revoke) | count=0         |

**Byte layout:**
```
Offset  Hex                                        Notes
00      04 00                                      blockLength = 4 (LE)
02      2C 00                                      templateId = 44 (LE)
04      78 00                                      schemaId = 120 (LE)
06      01 00                                      version = 1 (LE)
08      88 77 66 55 44 33 22 11                    sessionId (LE)
16      11 10 0F 0E 0D 0C 0B 0A                    correlationId (LE)
24      07 00 00 00                                generationId = 7 (LE)
28      00 00                                      assignment count = 0 (empty = revoke)
```
