# Mini-Kafka

"A from-scratch Kafka broker implementing the real wire protocol, KRaft metadata parsing, and an incrementally-maintained binary-search index over a single-segment log — with Produce-side record validation, rolling segments, and multi-broker replication explicitly out of scope."

## Implementation Status

### Fully Implemented
- **Event-driven TCP server** with fragmentation-safe message framing
- **Custom binary protocol parser/serializer** (UVarInt, CompactString, CompactArray)
- **KRaft metadata log parsing** (topic/partition/UUID extraction from raw disk bytes)
- **Lazily-built, incrementally-maintained in-memory index** (binary search, O(log N) lookup)
- **Accurate high watermark tracking**
- **max_bytes-bounded Fetch responses** (cleanly truncates at batch boundaries with 1-batch guarantee)
- **Clean error handling** for unknown topic/partition (UNKNOWN_TOPIC_OR_PARTITION, code 3)
- **Malformed/truncated batch payload protection** (bounds-checked parser, rejects incomplete batches)
- **Synchronous, single-threaded Produce path** — atomic by construction (no interleaving race between index update and disk write)

### Simplified / Shortcut (by design, disclosed)
- **Produce is a byte-pipe:** no CRC32C validation, no server-assigned sequential offsets, trusts client-provided offsets/structure beyond the bounds check
- **Single static active segment** — no rolling segment creation/rotation
- **Index is rebuilt via full disk scan on cold start / first access** per partition (not persisted to a `.index` file)
- **KRaft metadata is read and reported**, but the broker does not participate in actual Raft consensus or leader election

### Out of Scope
- **Multi-broker replication / Raft consensus**
- **Consumer group coordination** (FindCoordinator, JoinGroup, SyncGroup, `__consumer_offsets`)
- **Security** (SASL/TLS)
- **Log compaction / retention policies**
