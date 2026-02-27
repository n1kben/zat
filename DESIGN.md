# ZatDB Design

ZatDB is an embedded datom log engine. It provides an append-only log of immutable facts (datoms), a schema system to define and validate them, and bitemporal time tracking. It does not include indexes or a query engine — those are built by consumers on top of the log.

## Datom

The fundamental unit of data. A datom is an immutable fact:

```
[Entity, Attribute, Value, Tx, Op]
```

- **Entity (E)** — integer ID identifying the thing being described
- **Attribute (A)** — integer ID referencing a schema-defined attribute (interned)
- **Value (V)** — the value, typed according to the attribute's schema definition
- **Tx** — transaction ID this datom belongs to
- **Op** — `assert` or `retract`

## Transactions

A transaction is the unit of atomicity. All datoms in a transaction are committed together or not at all.

Transaction metadata:

```
[tx-id, system-time, valid-time]
```

- **tx-id** — monotonically increasing integer, assigned by the db
- **system-time** — when the transaction was recorded (assigned by the db, immutable)
- **valid-time** — when the facts are true in the real world (user-supplied, defaults to current time)

This gives ZatDB **bitemporal** time tracking:
- System-time answers: "when did the database learn this?"
- Valid-time answers: "when is/was this true?"
- Together they answer: "what did we think was true at time X, based on what we knew at time Y?"

## Schema

Attributes must be defined before use. Each attribute is an entity with:

- `:db/ident` — keyword name (e.g. `:user/name`)
- `:db/valueType` — one of the supported value types
- `:db/cardinality` — `:db.cardinality/one` or `:db.cardinality/many`

Schema is stored as datoms in the log (self-describing). Bootstrap schema attributes describe themselves.

### Schema rules

- **Add-only** — new attributes can be defined at any time
- **Immutable** — an attribute's type and cardinality cannot be changed after creation
- **Deprecation** — attributes can be deprecated via `:db/deprecated`. Deprecated attributes reject new assertions but existing datoms remain valid and queryable. Retractions on deprecated attributes are still allowed. Deprecation is irreversible.

### Schema loading

A schema snapshot is persisted in the file's meta/footer area. On startup, the snapshot is loaded directly (no log scanning required). The snapshot is updated whenever a schema-modifying transaction commits.

An in-memory schema cache is maintained for write-time validation.

## Entity IDs

Auto-assigned monotonic integers. The database owns the ID space. Transactions use **tempids** that get resolved to permanent IDs on commit:

```
User submits:
  [tempid-1, :user/name, "Alice"]
  [tempid-1, :user/age, 30]

DB resolves and stores:
  [1001, 100, "Alice", tx5, assert]
  [1001, 101, 30, tx5, assert]
```

Tempids within a transaction resolve consistently (same tempid = same entity).

## Attribute interning

Attributes like `:user/name` are entities with integer IDs. In stored datoms, the A position is the attribute's entity ID, not the string. Compact, fast comparisons.

## Value types

| Type                 | Description            |
|----------------------|------------------------|
| `:db.type/integer`   | 64-bit signed integer  |
| `:db.type/float`     | 64-bit float           |
| `:db.type/string`    | UTF-8 string           |
| `:db.type/boolean`   | true/false             |
| `:db.type/keyword`   | Interned keyword       |
| `:db.type/ref`       | Reference to entity ID |
| `:db.type/bytes`     | Raw byte array         |
| `:db.type/instant`   | Timestamp              |

## Storage

### Single file format

Everything is stored in a single `.zat` file:

```
+---------------------------+
| File Header               |  Magic bytes, version, config
+---------------------------+
| Log Segments              |  Append-only sequence of segments
|  +----------------------+ |
|  | Segment Header       | |  first_tx_id, tx_count, used_bytes
|  | Tx Header            | |  tx_id, system_time, valid_time, datom_count
|  | Datom                | |  [E, A, V, Op]
|  | Datom                | |  ...
|  | Tx Header            | |
|  | Datom                | |
|  | ...                  | |
|  +----------------------+ |
|  | Segment              | |
|  | ...                  | |
+---------------------------+
| Meta / Footer             |  last_tx_id, sparse index, schema snapshot
+---------------------------+
```

### Segmented log

The log is split into fixed-size segments (pages). Each segment contains one or more transactions. If a transaction doesn't fit in the current segment, a new one is started. Oversized transactions span multiple segments.

### Sparse index

A small array mapping `(first-tx-id-in-segment, segment-offset)`. One entry per segment. Used to locate a transaction by ID via binary search. Persisted in the meta/footer area. Fits in memory.

### Crash safety

Write transaction to log, fsync, then update meta/footer. If crash occurs before meta update, uncommitted bytes are ignored on recovery (meta still points to previous valid tx).

## API

### Zig API (primary)

**Write:**

```zig
var tx = try db.begin();
tx.setValidTime(timestamp);  // optional

const alice = tx.tempid();
try tx.add(alice, attr_name, Value.string("Alice"));
try tx.add(alice, attr_age, Value.int(30));

const result = try tx.commit();
const alice_id = result.resolve(alice);
```

**Read — log iterator:**

```zig
var it = db.log(.{});                     // from beginning
var it = db.log(.{ .from_tx = 500 });     // from tx 500

while (it.next()) |tx_record| {
    tx_record.id;
    tx_record.system_time;
    tx_record.valid_time;

    for (tx_record.datoms()) |datom| {
        datom.e;
        datom.a;
        datom.v;
        datom.op;
    }
}
```

**Read — single tx lookup:**

```zig
const tx_record = try db.tx(500);
```

**Schema access:**

```zig
const schema = db.schema();
const attr = schema.get(attr_id);   // id -> ident, type, cardinality, deprecated
const id = schema.resolve(":user/name");  // ident -> id
```

### C API (for FFI)

Thin wrapper over the Zig API for consumption from Node.js, Python, Ruby, etc. via shared library (`libzatdb.so` / `libzatdb.dylib`).

```c
// Lifecycle
zatdb *zatdb_open(const char *path);
void zatdb_close(zatdb *db);

// Write
zatdb_tx *zatdb_tx_begin(zatdb *db);
int zatdb_tx_set_valid_time(zatdb_tx *tx, int64_t valid_time);
int zatdb_tx_add(zatdb_tx *tx, int64_t e, int64_t a, zatdb_value val, zatdb_op op);
int zatdb_tx_add_tempid(zatdb_tx *tx, int32_t tempid, int64_t a, zatdb_value val);
zatdb_tx_result zatdb_tx_commit(zatdb_tx *tx);
void zatdb_tx_abort(zatdb_tx *tx);

// Schema
int zatdb_schema_define(zatdb_tx *tx, const char *ident,
                        zatdb_value_type type, zatdb_cardinality card);
int zatdb_schema_deprecate(zatdb_tx *tx, const char *ident);

// Read — log iterator
zatdb_log_iter *zatdb_log(zatdb *db, int64_t from_tx);
int zatdb_log_next(zatdb_log_iter *it, zatdb_tx_record *out);
void zatdb_log_datoms(zatdb_tx_record *tx, zatdb_datom *buf, int *count);
void zatdb_log_iter_free(zatdb_log_iter *it);

// Read — single tx lookup
int zatdb_tx_get(zatdb *db, int64_t tx_id, zatdb_tx_record *out);

// Schema access
int zatdb_schema_attr(zatdb *db, int64_t attr_id, zatdb_attr_info *out);
int zatdb_schema_resolve(zatdb *db, const char *ident, int64_t *attr_id);

// Value constructors
zatdb_value zatdb_value_int(int64_t v);
zatdb_value zatdb_value_string(const char *s, size_t len);
zatdb_value zatdb_value_bool(int v);
// ...
```

**C API design principles:**
- Opaque pointers — consumers never see internal structs
- Error codes — return int (0 = success, negative = error)
- Library owns memory — caller gets views/copies
- `zatdb_tx_result` contains tempid → resolved entity-id mapping

**Build outputs:**
- `libzatdb.so` / `libzatdb.dylib` — shared library
- `zatdb.h` — C header
- Zig module for direct Zig consumers

## Scope

**In scope (v1):**
- Datom log with append-only storage
- Bitemporal transactions (system-time + valid-time)
- Schema definition, validation, deprecation
- Segmented log with sparse index
- Log iterator for consumers
- Schema snapshot for fast startup
- Zig API + C API
- Single-file `.zat` format
- Crash safety via ordered writes + fsync

**Out of scope (for consumers to build):**
- Indexes (EAV, AVE, VAE, etc.)
- Query engine / Datalog
- Full-text search
- Replication
- Multi-process concurrency (v1 is single-process)
