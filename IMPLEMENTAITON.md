# ZatDB: Technical Implementation Design

> Post-research revision incorporating findings from storage engine analysis,
> prior art study (Mentat, Datalevin, Datahike, XTDB v2), and index optimization research.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Binary Encodings](#2-binary-encodings)
3. [File Format Specification](#3-file-format-specification)
4. [COW B+ Tree Implementation](#4-cow-b-tree-implementation)
5. [Index Architecture](#5-index-architecture)
6. [Transaction Engine](#6-transaction-engine)
7. [Query Engine](#7-query-engine)
8. [Concurrency Model](#8-concurrency-model)
9. [Implementation Plan — Step by Step](#9-implementation-plan)
10. [Test Strategy](#10-test-strategy)

---

## 1. Architecture Overview

### Revised Architecture (Post-Research)

Based on research findings, we're replacing the original LSM-inspired segment model
with an **LMDB-style copy-on-write B+ tree**. Key reasons:

- **No WAL needed** — dual meta pages provide crash safety
- **Zero-copy reads** — mmap gives pointer-stable access to all committed data
- **Natural MVCC** — COW pages mean old snapshots remain valid until unreferenced
- **Simpler implementation** — no segment management, no compaction scheduler, no merge logic

### Component Diagram

```
┌──────────────────────────────────────────────────┐
│                   User API Layer                  │
│  db.transact()  db.query()  db.pull()  db.asOf() │
├──────────────────────────────────────────────────┤
│              Transaction Processor                │
│  tempid resolution → schema validation →         │
│  datom generation → index writes → tx log        │
├──────────────────────────────────────────────────┤
│                  Query Engine                     │
│  Datalog parser → planner → LeapfrogTrieJoin →   │
│  index iterators → result assembly               │
├──────────────────────────────────────────────────┤
│              Index Manager (3 indexes)            │
│  EAV (primary)    AVE (selective)    VAE (refs)   │
│  + Transaction Log index                         │
├──────────────────────────────────────────────────┤
│            COW B+ Tree Engine                     │
│  page alloc → split/merge → COW commit →         │
│  dual meta page → free page tracking             │
├──────────────────────────────────────────────────┤
│              Page Manager / mmap                  │
│  file open → mmap → page read (zero-copy) →      │
│  page write (pwrite + remap) → fsync             │
├──────────────────────────────────────────────────┤
│               Single .zat File                    │
└──────────────────────────────────────────────────┘
```

---

## 2. Binary Encodings

### 2.1 Entity ID Layout

Entity IDs are u64, partitioned by convention (not enforced in storage):

```
Bits 63..54 (10 bits) — partition tag
Bits 53..0  (54 bits) — sequence within partition

Partition 0:  :db.part/db      (schema entities, attribute definitions)
Partition 1:  :db.part/tx      (transaction entities)
Partition 2:  :db.part/user    (user entities — default)
```

In practice:

```zig
const PARTITION_DB   = 0;
const PARTITION_TX   = 1;
const PARTITION_USER = 2;

fn makeEntityId(partition: u10, seq: u54) u64 {
    return (@as(u64, partition) << 54) | seq;
}

fn partitionOf(eid: u64) u10 {
    return @truncate(eid >> 54);
}
```

### 2.2 Value Encoding

Values are serialized as a type-tag byte followed by type-specific payload:

```
Tag  Type       Payload
0x00 nil        (0 bytes)
0x01 boolean    1 byte (0x00 = false, 0x01 = true)
0x02 i64        8 bytes, big-endian (for sortable comparison)
0x03 f64        8 bytes, IEEE 754 with sign-flip for sortability
0x04 string     4-byte length (BE) + UTF-8 bytes
0x05 keyword    4-byte length (BE) + UTF-8 bytes (":ns/name")
0x06 ref        8 bytes, big-endian u64 entity ID
0x07 instant    8 bytes, big-endian i64 (epoch microseconds)
0x08 uuid       16 bytes, big-endian
0x09 bytes      4-byte length (BE) + raw bytes
```

**Sortable float encoding** (for AVET index comparisons):

```zig
fn encodeSortableF64(val: f64) u64 {
    const bits = @bitCast(u64, val);
    if (bits >> 63 == 1) {
        return ~bits;           // negative: flip all bits
    } else {
        return bits ^ (1 << 63); // positive: flip sign bit
    }
}
```

### 2.3 Datom Key Encoding (per-index)

Each index stores datoms as key-value pairs in the B+ tree. We use
**DUPSORT-style encoding**: the leading component is the B+ tree key,
and the remaining components are packed into the value, stored in
sorted order within that key's leaf entries.

**EAV Index:**
```
Key:    [E: 8 bytes BE]
Value:  [A: 8 bytes BE][value_tag: 1 byte][value_payload: variable][Tx: 8 bytes BE][Op: 1 byte]
```

**AVE Index** (selective — only indexed/unique attributes):
```
Key:    [A: 8 bytes BE]
Value:  [value_tag: 1 byte][value_payload: variable][E: 8 bytes BE]
```

**VAE Index** (ref attributes only):
```
Key:    [V(ref): 8 bytes BE]
Value:  [A: 8 bytes BE][E: 8 bytes BE]
```

**Tx Log Index:**
```
Key:    [Tx: 8 bytes BE]
Value:  [E: 8 bytes BE][A: 8 bytes BE][value_tag: 1 byte][value_payload: variable][Op: 1 byte]
```

Note: Transaction IDs are NOT stored in EAV/AVE/VAE indexes for space
efficiency. They are only in the Tx Log. Time-travel queries consult the
Tx Log to filter. (This follows Datalevin's approach for ~24 bytes/datom savings.)

### 2.4 Key Comparison Functions

Each index needs a custom comparison function for B+ tree ordering.
All integers are big-endian, so bytewise comparison works for numeric types.
Strings compare lexicographically. Composite keys compare field by field.

```zig
/// Compare two EAV keys (entity IDs, big-endian)
fn compareEavKey(a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, a[0..8], b[0..8]);
}

/// Compare two EAV values (A + encoded value + Tx + Op)
fn compareEavValue(a: []const u8, b: []const u8) std.math.Order {
    // First compare attribute (bytes 0..8)
    const attr_cmp = std.mem.order(u8, a[0..8], b[0..8]);
    if (attr_cmp != .eq) return attr_cmp;
    // Then compare encoded value (variable length, type-tag + payload)
    // ... type-aware comparison
}
```

---

## 3. File Format Specification

### 3.1 File Layout

```
Offset 0                                   Offset page_size
┌──────────────┬──────────────┬───────────────┬───────────────┬─────────────────┐
│  Meta Page 0 │  Meta Page 1 │  Free DB Root │  Index Pages  │  ... more pages │
│  (page 0)    │  (page 1)    │  (page 2+)    │  (page N+)    │                 │
└──────────────┴──────────────┴───────────────┴───────────────┴─────────────────┘
```

### 3.2 Meta Page Format (page 0 and page 1)

Two meta pages alternate in a double-buffer scheme. On commit, the
older meta page is overwritten with new root pointers. On crash recovery,
the meta page with the higher valid tx_id wins.

```
Offset  Size  Field
0       4     magic: 0x5A415444 ("ZATD")
4       4     version: u32 (1)
8       4     page_size: u32 (default: OS page size)
12      4     flags: u32 (reserved)
16      8     tx_id: u64 (transaction counter, monotonically increasing)
24      8     eav_root_page: u64 (page number of EAV B+ tree root)
32      8     ave_root_page: u64 (page number of AVE B+ tree root)
40      8     vae_root_page: u64 (page number of VAE B+ tree root)
48      8     txlog_root_page: u64 (page number of Tx Log B+ tree root)
56      8     free_root_page: u64 (page number of free-page B+ tree root)
64      8     next_entity_id: u64 (next available entity ID per partition)
72      8     next_page: u64 (next unallocated page number = file size / page_size)
80      8     datom_count: u64 (total datoms across all indexes)
88      4     checksum: u32 (CRC-32C of bytes 0..87)
92      ...   (padding to page_size)
```

**Commit protocol:**

```
1. Write all new/modified pages via pwrite() — these are COW copies
2. fsync() the file
3. Write the NEW meta page (overwriting the older of the two)
4. fsync() the file again
```

If crash occurs before step 3: old meta page is still valid, new pages are
orphaned but harmless. If crash occurs during step 3: checksum validation
detects partial write, falls back to the other meta page.

### 3.3 Page Header (all page types)

Every page starts with an 8-byte header:

```
Offset  Size  Field
0       1     page_type: u8
                0x01 = Branch (interior B+ tree node)
                0x02 = Leaf (B+ tree leaf)
                0x03 = Overflow (large value continuation)
                0x04 = Free (on free list)
1       1     index_id: u8 (0=EAV, 1=AVE, 2=VAE, 3=TxLog, 4=Free)
2       2     num_entries: u16
4       4     reserved: u32
```

### 3.4 Branch Page Layout

```
[Header: 8 bytes]
[right_child_page: 8 bytes]                    // rightmost child pointer
[entry_offsets: num_entries × 2 bytes]         // offset within page to each entry
[entries...]                                    // packed entries:
    [child_page: 8 bytes][key_len: 2 bytes][key_data: variable]
[free space]
```

Entries are sorted by key. Binary search on entry_offsets for navigation.
Separator keys are the smallest key in the right subtree (fence keys).

### 3.5 Leaf Page Layout

```
[Header: 8 bytes]
[prev_leaf: 8 bytes]                           // page number of previous leaf (0 = none)
[next_leaf: 8 bytes]                           // page number of next leaf (0 = none)
[entry_offsets: num_entries × 2 bytes]         // offset within page to each entry
[entries...]                                    // packed entries (depends on index):
    [key_len: 2 bytes][key_data][val_len: 2 bytes][val_data]
[free space]
```

Leaf pages form a doubly-linked list for efficient range scans.

### 3.6 Overflow Pages

Values exceeding `(page_size - header - overhead) / 4` bytes (~1000 bytes
for 4KB pages) are stored in overflow pages:

```
[Header: 8 bytes]
[next_overflow_page: 8 bytes]                  // 0 = last page
[data_len: 4 bytes]                            // bytes of payload in this page
[data: variable]
```

The leaf entry stores a special marker: `[0xFF][overflow_page: 8 bytes][total_len: 4 bytes]`

---

## 4. COW B+ Tree Implementation

### 4.1 Core Operations

The B+ tree is the central data structure. Every write creates new page copies
(copy-on-write) instead of modifying existing pages. This enables:
- Crash safety without WAL
- Lock-free readers (they see old pages until mapping updates)
- Natural snapshots (save a root page number = saved database state)

#### Insert (COW path)

```
insert(root, key, value):
  1. path = find_leaf(root, key)           // descend from root, recording path
  2. leaf = cow_copy(path.leaf)            // copy leaf page to new page
  3. insert_into_leaf(leaf, key, value)
  4. if leaf is full:
       (left, right, separator) = split_leaf(leaf)
       propagate split upward through path (COW-copying each ancestor)
  5. else:
       propagate new leaf page number upward (COW-copying each ancestor)
  6. return new_root_page                  // caller stores in meta page
```

#### Lookup

```
lookup(root, key):
  1. page = read_page(root)               // zero-copy via mmap
  2. while page.type == Branch:
       idx = binary_search(page.entries, key)
       child = page.entries[idx].child_page
       page = read_page(child)
  3. idx = binary_search(page.entries, key)
  4. if page.entries[idx].key == key:
       return page.entries[idx].value
  5. return null
```

#### Range Scan

```
range_scan(root, start_key, end_key):
  1. leaf = find_leaf(root, start_key)
  2. idx = first entry >= start_key in leaf
  3. yield entries while key <= end_key
  4. when leaf exhausted: follow next_leaf pointer
```

### 4.2 Free Page Management

The FreeDB is itself a B+ tree (index_id=4) mapping:

```
Key:    [tx_id: 8 bytes BE]          // transaction that freed the pages
Value:  [page_count: 4 bytes][page_numbers: page_count × 8 bytes]
```

**Page allocation priority:**
1. Reuse from FreeDB (oldest tx first, only if no active readers reference that tx)
2. Append new page at end of file (increment `next_page` in meta)

**Page freeing:** When a COW operation replaces a page, the old page number
is added to a "pending free" list. On commit, the list is written to the
FreeDB keyed by the current tx_id.

### 4.3 Split Strategy

**Leaf split:** 50/50 by default. When inserting in append-order (monotonically
increasing keys, common for entity IDs), detect the pattern and use 90/10 split
(keep 90% in old page, 10% in new) to maximize page fill factor. This is
LMDB's `MDB_APPEND` optimization.

**Branch split:** Always 50/50 (separator key promoted to parent).

### 4.4 Page Size and Fill Factor

```zig
const Config = struct {
    page_size: u32 = 0,       // 0 = auto-detect OS page size
    min_fill: f32 = 0.4,      // merge threshold
    max_entries_per_leaf: u16 = 0, // 0 = compute from page_size
};

fn detectPageSize() u32 {
    // Linux: getpagesize() or sysconf(_SC_PAGESIZE)
    // macOS/Apple Silicon: 16384
    // Windows: GetSystemInfo().dwPageSize
    // Fallback: 4096
    return std.os.page_size;
}
```

---

## 5. Index Architecture

### 5.1 Three Indexes + Tx Log (Revised from Datalevin findings)

The research showed that Datalevin dropped AEV (unused in practice) and
stores tx IDs separately. We adopt this:

| Index  | Key       | Sorted Values (DUPSORT)     | Scope                | Purpose                          |
|--------|-----------|-----------------------------|----------------------|----------------------------------|
| EAV    | Entity    | (Attr, Value)               | All datoms           | Entity lookup, Pull API          |
| AVE    | Attribute | (Value, Entity)             | `:db/index` = true   | Value lookup, uniqueness         |
| VAE    | Value(ref)| (Attribute, Entity)         | `:db.type/ref` only  | Reverse reference traversal      |
| TxLog  | Tx ID     | (Entity, Attr, Value, Op)   | All datoms           | Time travel, audit, tx metadata  |

### 5.2 DUPSORT Encoding Detail

"DUPSORT" means: within a single B+ tree key, there can be multiple sorted values.
We implement this as a two-level structure within leaf pages:

```
Leaf page for EAV index:
  ┌─────────────────────────────────────────────┐
  │ Key: Entity 1001                             │
  │   Dup 0: [attr=:user/name][val="Alice"]      │
  │   Dup 1: [attr=:user/age][val=32]            │
  │   Dup 2: [attr=:user/email][val="a@b.com"]   │
  ├─────────────────────────────────────────────┤
  │ Key: Entity 1002                             │
  │   Dup 0: [attr=:user/name][val="Bob"]        │
  │   ...                                        │
  └─────────────────────────────────────────────┘
```

Entity 1001 is stored ONCE as the key, not repeated for each datom.
For entities with many attributes, this saves significant space.

**Leaf entry format with DUPSORT:**

```
[key_len: 2][key_data: 8 bytes (entity ID)]
[dup_count: 2]
[dup_offsets: dup_count × 2 bytes]
[dup_entries...]
    [dup_len: 2][dup_data: variable (attr + value encoding)]
```

When `dup_count` exceeds a threshold (~50), the duplicates overflow to a
**sub-tree** — a separate B+ tree rooted at an overflow page, keyed by
the duplicate values. This handles entities with hundreds of attributes efficiently.

### 5.3 Schema Attribute Cache

To avoid B+ tree lookups for every attribute validation, maintain an in-memory
cache of schema attributes:

```zig
const SchemaAttr = struct {
    eid: u64,                  // attribute entity ID
    ident: []const u8,         // :namespace/name
    value_type: ValueType,
    cardinality: Cardinality,
    unique: ?Uniqueness,
    indexed: bool,             // maintain AVE index entry
    is_component: bool,
    doc: ?[]const u8,
};

const SchemaCache = struct {
    by_eid: std.AutoHashMap(u64, SchemaAttr),
    by_ident: std.StringHashMap(u64),

    pub fn load(eav_tree: *BPlusTree) !SchemaCache {
        // Scan all entities in partition 0 (:db.part/db)
        // Reconstruct SchemaAttr records from EAV datoms
    }
};
```

### 5.4 Bootstrap Transaction

Transaction 0 installs the meta-schema. This is hardcoded:

```zig
const BOOTSTRAP_DATOMS = [_]Datom{
    // :db/ident attribute (entity 1)
    .{ .e = 1, .a = 1, .v = .{ .keyword = ":db/ident" }, .tx = 0, .op = true },
    // :db/valueType attribute (entity 2)
    .{ .e = 2, .a = 1, .v = .{ .keyword = ":db/valueType" }, .tx = 0, .op = true },
    .{ .e = 2, .a = 2, .v = .{ .keyword = ":db.type/keyword" }, .tx = 0, .op = true },
    // :db/cardinality attribute (entity 3)
    .{ .e = 3, .a = 1, .v = .{ .keyword = ":db/cardinality" }, .tx = 0, .op = true },
    .{ .e = 3, .a = 2, .v = .{ .keyword = ":db.type/keyword" }, .tx = 0, .op = true },
    // ... (all meta-attributes define themselves)
    // :db/index, :db/unique, :db/doc, :db/isComponent, :db/txInstant
};
```

---

## 6. Transaction Engine

### 6.1 Transaction Processing Pipeline

```
User input: list of operations
  │
  ▼
┌─────────────────────────┐
│ 1. Parse & Normalize    │  Convert maps → flat [op, e, a, v] operations
│                         │  Resolve keyword attrs → entity IDs via schema cache
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ 2. Tempid Resolution    │  Allocate real entity IDs for temp IDs
│                         │  Group operations by tempid for resolution
│                         │  :db.unique/identity → upsert (lookup existing)
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ 3. Schema Validation    │  Type-check values against :db/valueType
│                         │  Enforce :db/cardinality/one (retract old if exists)
│                         │  Verify attribute exists in schema cache
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ 4. Unique Constraints   │  For :db/unique attrs: check AVE index
│                         │  :db.unique/value → error if conflict
│                         │  :db.unique/identity → upsert resolution
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ 5. Datom Generation     │  Produce final datom list:
│                         │  - Explicit assertions (op=true)
│                         │  - Explicit retractions (op=false)
│                         │  - Implicit retractions (cardinality/one replacement)
│                         │  - Tx entity datom: [:db/txInstant now]
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ 6. Index Writes (COW)   │  Insert each datom into relevant indexes:
│                         │  - ALL datoms → EAV
│                         │  - ALL datoms → TxLog
│                         │  - Indexed attrs → AVE
│                         │  - Ref attrs → VAE
│                         │  Each insert produces COW page chain
│                         │  Collect new root page numbers
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ 7. Commit               │  Write all dirty pages via pwrite()
│                         │  fsync()
│                         │  Write new meta page with new roots
│                         │  fsync()
│                         │  Update schema cache if schema changed
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ 8. Return TxReport      │  { db_before, db_after, tx_id, tempids, datoms }
└─────────────────────────┘
```

### 6.2 Cardinality Enforcement

For `:db/cardinality/one` attributes, asserting a new value must retract the old:

```zig
fn processCardinalityOne(
    eav_tree: *BPlusTree,
    entity: u64,
    attr: u64,
    new_value: Value,
    datoms: *DatomList,
) !void {
    // Look up existing value in EAV index
    var iter = eav_tree.seek(.{ .e = entity, .a = attr });
    if (iter.next()) |existing| {
        if (!existing.value.eql(new_value)) {
            // Retract old value
            datoms.append(.{
                .e = entity, .a = attr, .v = existing.value,
                .tx = current_tx, .op = false,
            });
        }
    }
    // Assert new value
    datoms.append(.{
        .e = entity, .a = attr, .v = new_value,
        .tx = current_tx, .op = true,
    });
}
```

### 6.3 Tx Entity

Every transaction is itself an entity (in `:db.part/tx` partition):

```zig
fn createTxEntity(tx_id: u64, datoms: *DatomList) void {
    datoms.append(.{
        .e = tx_id,
        .a = DB_TX_INSTANT,
        .v = .{ .instant = std.time.microTimestamp() },
        .tx = tx_id,
        .op = true,
    });
}
```

Users can assert additional attributes on the tx entity for audit metadata:

```zig
db.transact(&.{
    .{ .db_add, "user-1", ":user/name", .{ .string = "Alice" } },
    .{ .db_add, .db_tx, ":audit/user", .{ .string = "admin" } },
    .{ .db_add, .db_tx, ":audit/reason", .{ .string = "initial load" } },
});
```

---

## 7. Query Engine

### 7.1 Datalog AST

```zig
const Query = struct {
    find: []FindElem,           // :find clause
    where: []Clause,            // :where clause
    in: []InputVar,             // :in clause ($ = default db)
    rules: []Rule,              // rule definitions
    with: []Symbol,             // :with clause (prevent duplicate elimination)
};

const FindElem = union(enum) {
    variable: Symbol,           // ?name
    aggregate: Aggregate,       // (count ?name)
    pull: PullExpr,             // (pull ?e [:user/name])
};

const Clause = union(enum) {
    pattern: PatternClause,     // [?e :user/name ?name]
    predicate: PredClause,      // [(> ?age 30)]
    fn_expr: FnClause,          // [(str/upper ?name) ?upper]
    rule_call: RuleCall,        // (ancestor ?p ?a)
    or_clause: [][]Clause,      // (or ...)
    not_clause: []Clause,       // (not ...)
};

const PatternClause = struct {
    entity: PatternComponent,   // ?e or literal entity ID
    attribute: PatternComponent,// :user/name or ?a
    value: PatternComponent,    // ?name or literal value
    source: ?Symbol,            // $ (default) or named source
};

const PatternComponent = union(enum) {
    variable: Symbol,
    constant: Value,
    blank: void,                // _ (ignore)
};
```

### 7.2 Datalog Parser

The parser handles EDN-like syntax. Implementation strategy: hand-written
recursive descent (no parser combinator library — keep dependencies at zero).

```
Input:  "[:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age] [(> ?age 30)]]"

Tokens: LBRACKET, COLON, IDENT("find"), VARIABLE("?name"), VARIABLE("?age"),
        COLON, IDENT("where"),
        LBRACKET, VARIABLE("?e"), KEYWORD(":user/name"), VARIABLE("?name"), RBRACKET,
        LBRACKET, VARIABLE("?e"), KEYWORD(":user/age"), VARIABLE("?age"), RBRACKET,
        LBRACKET, LPAREN, GT, VARIABLE("?age"), INT(30), RPAREN, RBRACKET,
        RBRACKET

AST:    Query {
            find: [Variable("?name"), Variable("?age")],
            where: [
                Pattern { e: Var("?e"), a: Const(:user/name), v: Var("?name") },
                Pattern { e: Var("?e"), a: Const(:user/age), v: Var("?age") },
                Predicate { op: >, args: [Var("?age"), Const(30)] },
            ],
        }
```

### 7.3 Query Planning

#### Variable Binding Analysis

Determine which variables are bound at each step, which index to use
for each pattern clause, and the optimal evaluation order.

```
Clause 1: [?e :user/name ?name]
  - Bound: (none initially)
  - Provides: {?e, ?name}
  - Best index: AVE (attribute is constant, scan all entities with :user/name)

Clause 2: [?e :user/age ?age]
  - Bound after clause 1: {?e, ?name}
  - ?e is bound → use EAV index: seek(entity=?e, attr=:user/age)
  - Provides: {?age}

Clause 3: [(> ?age 30)]
  - Bound: {?e, ?name, ?age}
  - Filter predicate, applied in memory
```

#### Cost Estimation

For each clause, estimate result size:

```zig
fn estimateCost(clause: PatternClause, bound_vars: VarSet, db: *Database) u64 {
    const attr = resolveAttr(clause.attribute) orelse return db.datom_count;

    if (clause.entity.isConstant()) {
        // Point lookup: ~1 result for cardinality/one, ~N for cardinality/many
        return if (attr.cardinality == .one) 1 else 10; // heuristic
    }

    if (clause.entity.isVariable() and bound_vars.contains(clause.entity.variable)) {
        // Entity bound from previous clause: point lookup
        return 1;
    }

    // Full attribute scan: use count from EAV index
    return db.countAttribute(attr.eid);
}
```

#### Clause Reordering

Sort clauses by estimated cost (cheapest first), respecting variable dependencies:

```zig
fn orderClauses(clauses: []Clause, db: *Database) []Clause {
    var ordered = std.ArrayList(Clause).init(allocator);
    var bound = VarSet.init();

    while (ordered.items.len < clauses.len) {
        var best_idx: usize = 0;
        var best_cost: u64 = std.math.maxInt(u64);

        for (clauses, 0..) |clause, i| {
            if (already_added(i)) continue;
            if (!dependencies_met(clause, bound)) continue;

            const cost = estimateCost(clause, bound, db);
            if (cost < best_cost) {
                best_cost = cost;
                best_idx = i;
            }
        }

        ordered.append(clauses[best_idx]);
        bound.addAll(providedVars(clauses[best_idx]));
    }
    return ordered.items;
}
```

### 7.4 Leapfrog TrieJoin (WCOJ)

For multi-pattern queries with cyclic joins (e.g., triangles, graph traversals),
implement LeapfrogTrieJoin. The key insight: B+ tree iterators with `seek(key)`
and `next()` naturally form tries.

```zig
const LeapfrogIter = struct {
    // Wraps a B+ tree iterator positioned at a specific variable level
    tree: *BPlusTree,
    index: IndexId,
    level: u8,              // which component of the key we're iterating
    current: ?Value,
    at_end: bool,

    pub fn seek(self: *LeapfrogIter, target: Value) void {
        // Position iterator at first key >= target at this level
    }

    pub fn next(self: *LeapfrogIter) void {
        // Advance to next distinct value at this level
    }

    pub fn key(self: *LeapfrogIter) Value {
        return self.current.?;
    }
};

/// Intersect N sorted iterators via leapfrog
fn leapfrogJoin(iters: []LeapfrogIter) Iterator([]Value) {
    // Sort iters by current key
    // Repeatedly: max = largest current key among all iters
    //   For each iter (round-robin from smallest):
    //     seek(max)
    //     if all iters now agree on same key → emit match
    //     else → max = new largest, continue
}
```

**When to use Leapfrog vs. simple nested-loop:**
- 2 pattern clauses sharing 1 variable → simple index lookup (nested loop)
- 3+ clauses, cyclic variable sharing → Leapfrog TrieJoin
- Single clause → direct index scan

### 7.5 Time-Travel Queries

Since tx IDs are stored in the TxLog (not in EAV/AVE/VAE), time-travel
requires a two-phase approach:

**as-of(tx_target):**
```
1. Query EAV/AVE/VAE normally (these contain current state)
2. For each result datom, verify it was asserted at tx <= tx_target
   by consulting the TxLog
3. Check no retraction exists for this (E,A,V) with tx <= tx_target
   that occurs after the assertion tx
```

**Optimization:** For recent as-of queries (tx_target near latest), this is fast.
For old as-of queries, we may need to scan significant TxLog portions.

**Alternative (Phase 3 optimization):** Maintain a separate "historical EAV"
index that includes tx IDs. Trade space for time-travel query speed.

### 7.6 Pull API

```zig
pub fn pull(db: *Database, eid: u64, pattern: PullPattern) !PullResult {
    var result = PullResult.init(allocator);

    for (pattern.attrs) |attr_spec| {
        switch (attr_spec) {
            .attr => |ident| {
                const attr_id = db.schema.resolveIdent(ident);
                var iter = db.eav.seek(eid, attr_id);
                while (iter.nextForKey()) |datom| {
                    result.put(ident, datom.value);
                }
            },
            .join => |join| {
                // Recursively pull referenced entities
                const attr_id = db.schema.resolveIdent(join.ident);
                var iter = db.eav.seek(eid, attr_id);
                while (iter.nextForKey()) |datom| {
                    const ref_eid = datom.value.ref;
                    const sub = try pull(db, ref_eid, join.sub_pattern);
                    result.putNested(join.ident, sub);
                }
            },
        }
    }
    return result;
}
```

---

## 8. Concurrency Model

### 8.1 Single Writer, Multiple Readers

```zig
const Database = struct {
    file: std.fs.File,
    mmap: []align(std.mem.page_size) u8,
    mmap_len: usize,

    // Writer state (protected by write_mutex)
    write_mutex: std.Thread.Mutex,
    current_meta: *MetaPage,         // points into mmap
    dirty_pages: PageList,           // COW pages not yet committed

    // Reader state
    reader_slots: [MAX_READERS]ReaderSlot,  // atomic tx_id snapshots

    pub fn beginRead(self: *Database) ReadTx {
        const meta = self.latestMeta();     // read both meta pages, pick higher tx_id
        const slot = self.acquireReaderSlot();
        slot.tx_id.store(meta.tx_id, .release);
        return ReadTx{ .db = self, .meta = meta, .slot = slot };
    }

    pub fn beginWrite(self: *Database) !WriteTx {
        self.write_mutex.lock();
        const meta = self.latestMeta();
        return WriteTx{
            .db = self,
            .meta = meta,
            .dirty_pages = PageList.init(),
            .new_roots = meta.roots(),
        };
    }
};

const ReadTx = struct {
    db: *Database,
    meta: MetaPage,
    slot: *ReaderSlot,

    pub fn close(self: *ReadTx) void {
        self.slot.tx_id.store(0, .release); // release reader slot
    }
};
```

### 8.2 Reader Slot Table

To know which pages are safe to reclaim, we track the oldest active reader:

```zig
const MAX_READERS = 126;

const ReaderSlot = struct {
    tx_id: std.atomic.Value(u64),  // 0 = unused
    pid: std.atomic.Value(u32),    // for stale reader detection
};

fn oldestActiveReader(slots: []ReaderSlot) u64 {
    var oldest: u64 = std.math.maxInt(u64);
    for (slots) |slot| {
        const tx = slot.tx_id.load(.acquire);
        if (tx != 0 and tx < oldest) oldest = tx;
    }
    return oldest;
}
```

Pages freed by transactions newer than the oldest active reader's tx are
NOT yet reclaimable. Once all readers advance past a tx, its freed pages
enter the available pool.

---

## 9. Implementation Plan — Step by Step

### Legend
- 🧪 = has specific tests listed in Section 10
- ⏱️ = estimated effort (person-days)

---

### Step 0: Project Scaffolding ⏱️ 1 day

**What:** Set up Zig project structure, build system, test harness.

```
zatdb/
├── build.zig
├── build.zig.zon
├── src/
│   ├── main.zig               // Library root, public API
│   ├── page.zig               // Page types, header, serialization
│   ├── btree.zig              // B+ tree core
│   ├── encoding.zig           // Value encoding/decoding
│   ├── meta.zig               // Meta page read/write/validation
│   ├── file.zig               // File I/O, mmap management
│   ├── index.zig              // Index manager (EAV, AVE, VAE, TxLog)
│   ├── tx.zig                 // Transaction processor
│   ├── schema.zig             // Schema cache, bootstrap
│   ├── query/
│   │   ├── parser.zig         // Datalog parser
│   │   ├── planner.zig        // Query planner
│   │   ├── executor.zig       // Execution engine
│   │   └── leapfrog.zig       // LeapfrogTrieJoin
│   └── c_api.zig              // C FFI bindings
├── tests/
│   ├── test_encoding.zig
│   ├── test_btree.zig
│   ├── test_tx.zig
│   ├── test_query.zig
│   └── test_crash.zig
└── tools/
    ├── zat-dump.zig           // CLI: dump database contents
    └── zat-bench.zig          // CLI: benchmarks
```

**Done when:** `zig build test` runs with a placeholder test passing.

---

### Step 1: Value Encoding 🧪 ⏱️ 2 days

**Files:** `src/encoding.zig`

Implement:
- `encode(value: Value, buf: []u8) usize` — serialize value to bytes
- `decode(bytes: []const u8) Value` — deserialize
- `compareEncoded(a: []const u8, b: []const u8) Order` — bytewise sortable comparison
- `encodeSortableF64` / `decodeSortableF64`
- `encodeSortableI64` / `decodeSortableI64` (flip sign bit for signed sort)

**Key property:** `compareEncoded(encode(a), encode(b)) == compare(a, b)` for all values.

**Tests:** See 10.1

---

### Step 2: Page Primitives 🧪 ⏱️ 2 days

**Files:** `src/page.zig`

Implement:
- Page header read/write
- Leaf page: create, insert entry (sorted), lookup (binary search), split
- Branch page: create, insert separator, lookup child, split
- Overflow page: create chain, read chain
- `PageBuffer` — in-memory page with dirty tracking

```zig
const Page = struct {
    data: [*]align(page_size) u8,  // raw page bytes
    dirty: bool,

    pub fn header(self: *Page) *PageHeader { ... }
    pub fn leafEntryCount(self: *Page) u16 { ... }
    pub fn findInLeaf(self: *Page, key: []const u8, cmp: CmpFn) ?usize { ... }
    pub fn insertInLeaf(self: *Page, idx: usize, key: []const u8, val: []const u8) !void { ... }
    pub fn splitLeaf(self: *Page) !SplitResult { ... }
};
```

**Tests:** See 10.2

---

### Step 3: File Manager & mmap 🧪 ⏱️ 3 days

**Files:** `src/file.zig`, `src/meta.zig`

Implement:
- Open/create `.zat` file
- Write meta page 0 on creation (bootstrap empty database)
- mmap the file for reading
- `readPage(page_num) → *Page` via mmap pointer arithmetic
- `allocPage() → page_num` (extend file, update next_page)
- `writePage(page_num, data)` via pwrite
- Meta page read: validate checksum, pick higher tx_id
- Meta page write: compute checksum, write to older slot
- `remap()` — extend mmap after file grows
- **Platform abstraction:** Linux/macOS mmap, Windows pread/pwrite fallback

```zig
const FileManager = struct {
    file: std.fs.File,
    map: ?[]align(std.mem.page_size) u8,
    page_size: u32,
    file_size: u64,

    pub fn open(path: []const u8, opts: OpenOpts) !FileManager { ... }
    pub fn readPage(self: *FileManager, pgno: u64) *Page { ... }
    pub fn writePage(self: *FileManager, pgno: u64, data: []const u8) !void { ... }
    pub fn allocPage(self: *FileManager) !u64 { ... }
    pub fn sync(self: *FileManager) !void { ... }
    pub fn close(self: *FileManager) void { ... }
};
```

**Tests:** See 10.3

---

### Step 4: B+ Tree (Read Path) 🧪 ⏱️ 3 days

**Files:** `src/btree.zig`

Implement read-only operations first:
- `BPlusTree.init(root_page, file_mgr, cmp_fn)`
- `lookup(key) → ?value`
- `seek(key) → Iterator` (position at first entry >= key)
- `Iterator.next() → ?(key, value)`
- `Iterator.prev() → ?(key, value)` (for reverse scans)
- Range scan: `range(start, end) → Iterator`

```zig
const BPlusTree = struct {
    root: u64,              // root page number
    fm: *FileManager,
    key_cmp: CmpFn,
    val_cmp: ?CmpFn,       // for DUPSORT within a key

    pub fn lookup(self: *BPlusTree, key: []const u8) ?[]const u8 { ... }
    pub fn seek(self: *BPlusTree, key: []const u8) Iterator { ... }

    const Iterator = struct {
        tree: *BPlusTree,
        leaf: *Page,
        idx: u16,

        pub fn next(self: *Iterator) ?Entry { ... }
        pub fn key(self: *Iterator) []const u8 { ... }
        pub fn value(self: *Iterator) []const u8 { ... }
    };
};
```

**Tests:** See 10.4

---

### Step 5: B+ Tree (Write Path — COW) 🧪 ⏱️ 5 days

**Files:** `src/btree.zig` (extend)

This is the hardest single step. Implement:
- `insert(key, value) → new_root_page`
- COW page copying: allocate new page, copy, modify copy
- Leaf split (50/50 default, 90/10 for append-order detection)
- Branch split with separator promotion
- Path recording during descent (for COW propagation)
- `delete(key) → new_root_page` (for retractions)
- Page merge on underflow (optional in MVP — can defer)

**COW insert pseudocode:**

```zig
pub fn insert(self: *BPlusTree, key: []const u8, value: []const u8) !u64 {
    var path = self.descend(key);  // records [root, ..., leaf] page numbers

    // COW-copy the leaf
    var new_leaf = try self.fm.allocPage();
    self.fm.copyPage(new_leaf, path.leaf());
    insertEntry(new_leaf, key, value);

    if (pageOverflow(new_leaf)) {
        var split = splitLeaf(new_leaf);
        // Propagate split upward, COW-copying each ancestor
        return self.propagateSplit(&path, split);
    } else {
        // Just COW-copy ancestors with updated child pointer
        return self.propagateUpdate(&path, new_leaf);
    }
}
```

**Tests:** See 10.5

---

### Step 6: Free Page Tracking 🧪 ⏱️ 2 days

**Files:** `src/btree.zig` (extend), `src/file.zig` (extend)

Implement:
- FreeDB B+ tree (stores freed page lists keyed by tx_id)
- `freePage(pgno, tx_id)` — record a freed page
- `allocPage()` — check FreeDB first, fall back to file extension
- Handle the bootstrap problem: inserting into FreeDB may itself need pages

**Tests:** See 10.6

---

### Step 7: Schema & Bootstrap 🧪 ⏱️ 2 days

**Files:** `src/schema.zig`

Implement:
- Bootstrap: on new database, insert BOOTSTRAP_DATOMS into EAV
- SchemaCache: load from EAV on open, update on schema transactions
- Attribute resolution: keyword → entity ID
- Schema validation: value type checking

**Tests:** See 10.7

---

### Step 8: Index Manager 🧪 ⏱️ 3 days

**Files:** `src/index.zig`

Wire up the B+ trees as named indexes:

```zig
const IndexManager = struct {
    eav: BPlusTree,
    ave: BPlusTree,
    vae: BPlusTree,
    txlog: BPlusTree,
    free: BPlusTree,
    schema: SchemaCache,

    pub fn insertDatom(self: *IndexManager, d: Datom) !IndexRoots {
        var roots = self.currentRoots();
        roots.eav = try self.eav.insert(encodeEavKey(d), encodeEavVal(d));
        roots.txlog = try self.txlog.insert(encodeTxLogKey(d), encodeTxLogVal(d));
        if (self.schema.isIndexed(d.a)) {
            roots.ave = try self.ave.insert(encodeAveKey(d), encodeAveVal(d));
        }
        if (self.schema.isRef(d.a)) {
            roots.vae = try self.vae.insert(encodeVaeKey(d), encodeVaeVal(d));
        }
        return roots;
    }
};
```

**Tests:** See 10.8

---

### Step 9: Transaction Processor 🧪 ⏱️ 4 days

**Files:** `src/tx.zig`

Implement the full pipeline from Section 6:
- Tempid resolution
- Schema validation
- Cardinality/one enforcement (implicit retractions)
- Unique constraint checking (via AVE lookup)
- Datom generation
- Index writes (via IndexManager)
- Meta page commit (dual meta page protocol)
- TxReport construction

```zig
const TxProcessor = struct {
    db: *Database,
    allocator: Allocator,

    pub fn transact(self: *TxProcessor, ops: []const TxOp) !TxReport {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var datoms = DatomList.init(arena.allocator());
        const tx_id = self.db.nextTxId();

        // Pipeline stages
        const resolved = try self.resolveTempids(ops, arena.allocator());
        try self.validateSchema(resolved);
        try self.enforceCardinality(resolved, &datoms);
        try self.checkUniques(resolved);
        try self.generateDatoms(resolved, tx_id, &datoms);
        self.createTxEntity(tx_id, &datoms);

        // Write to indexes
        const new_roots = try self.db.index_mgr.insertAll(datoms.items);

        // Commit
        try self.db.commitMetaPage(tx_id, new_roots);

        return TxReport{
            .tx_id = tx_id,
            .tempids = resolved.tempid_map,
            .datoms = datoms.items,
        };
    }
};
```

**🎉 MILESTONE: After Step 9, you have a working embedded database that can:**
- Create a new .zat file
- Define schema attributes
- Transact data (assert, retract)
- Look up entities by ID via EAV
- Look up entities by value via AVE
- Traverse reverse references via VAE
- Survive crashes (dual meta page)

**Tests:** See 10.9

---

### Step 10: Datalog Parser 🧪 ⏱️ 3 days

**Files:** `src/query/parser.zig`

Hand-written lexer + recursive descent parser:
- Tokenizer: keywords (`:ns/name`), variables (`?x`), integers, strings, brackets, parens
- Parser: `[:find ... :where ...]` → Query AST
- Support: pattern clauses, predicate expressions, `:in` clause

Start with a subset:
```
Phase 1: [:find ?var+ :where [pattern-clause]+]
Phase 2: Add predicate clauses [(> ?x 10)]
Phase 3: Add :in clause, rule invocations
Phase 4: Add (or ...), (not ...), (not-join ...)
```

**Tests:** See 10.10

---

### Step 11: Basic Query Executor 🧪 ⏱️ 4 days

**Files:** `src/query/executor.zig`, `src/query/planner.zig`

Implement naive nested-loop join first (correct before fast):

```zig
fn executeQuery(db: *Database, query: Query) !ResultSet {
    const plan = planQuery(query, db);
    var bindings = BindingTable.init(allocator);

    for (plan.ordered_clauses) |clause| {
        switch (clause) {
            .pattern => |p| {
                bindings = try executePattern(db, p, bindings);
            },
            .predicate => |p| {
                bindings = try filterPredicate(p, bindings);
            },
        }
    }

    return projectResults(query.find, bindings);
}

fn executePattern(
    db: *Database,
    pattern: PatternClause,
    existing: BindingTable,
) !BindingTable {
    var result = BindingTable.init(allocator);

    if (existing.isEmpty()) {
        // First clause: full index scan
        const iter = chooseIndex(db, pattern);
        while (iter.next()) |datom| {
            if (matchesPattern(datom, pattern)) {
                result.addRow(extractBindings(datom, pattern));
            }
        }
    } else {
        // Subsequent clause: index lookup per existing binding
        for (existing.rows()) |row| {
            const bound_pattern = substituteBindings(pattern, row);
            const iter = chooseIndex(db, bound_pattern);
            while (iter.next()) |datom| {
                if (matchesPattern(datom, bound_pattern)) {
                    var new_row = row.clone();
                    new_row.merge(extractBindings(datom, pattern));
                    result.addRow(new_row);
                }
            }
        }
    }
    return result;
}
```

**Tests:** See 10.11

---

### Step 12: Time Travel 🧪 ⏱️ 3 days

**Files:** `src/query/executor.zig` (extend), `src/index.zig` (extend)

Implement:
- `db.asOf(tx_id) → FilteredDb` — wraps db with tx ceiling
- `db.since(tx_id) → FilteredDb` — wraps db with tx floor
- `db.history() → HistoryDb` — exposes all datoms including retractions
- TxLog scanning for time-travel validation

```zig
const FilteredDb = struct {
    inner: *Database,
    mode: TimeMode,
    tx_bound: u64,

    const TimeMode = enum { as_of, since, history };

    pub fn eav(self: *FilteredDb) FilteredIterator {
        // Wrap inner.eav iterator with tx-based filtering
    }
};
```

**Tests:** See 10.12

---

### Step 13: Concurrent Readers 🧪 ⏱️ 3 days

**Files:** `src/main.zig` (extend for reader slots), `src/file.zig` (extend)

Implement:
- Reader slot table (atomic operations)
- `beginRead()` / `endRead()` lifecycle
- Write mutex for single-writer serialization
- Free page reclamation respecting oldest active reader
- Stale reader detection (check PID validity)

**Tests:** See 10.13

---

### Step 14: Query Optimizer 🧪 ⏱️ 3 days

**Files:** `src/query/planner.zig` (extend)

Implement cost-based clause reordering:
- Cardinality estimation from index statistics
- Variable dependency graph
- Greedy ordering: cheapest satisfiable clause next
- Index selection: EAV vs AVE vs VAE based on bound variables

**Tests:** See 10.14

---

### Step 15: Leapfrog TrieJoin ⏱️ 4 days

**Files:** `src/query/leapfrog.zig`

Implement for 3+ clause queries with cyclic variable sharing:
- LeapfrogIter wrapping B+ tree iterator
- Intersection via leapfrog protocol
- Integration with query planner (detect when WCOJ is beneficial)
- Fallback to nested-loop for acyclic queries (where it's equally fast)

**Tests:** See 10.15

---

### Step 16: Pull API ⏱️ 2 days

**Files:** `src/query/pull.zig`

Implement as described in Section 7.6. Straightforward recursive
EAV index traversal with pattern-based attribute selection.

**Tests:** See 10.16

---

### Step 17: C API ⏱️ 3 days

**Files:** `src/c_api.zig`

Export stable C ABI:
- `zat_open`, `zat_close`
- `zat_tx_begin`, `zat_tx_add`, `zat_tx_retract`, `zat_tx_commit`
- `zat_query`, `zat_query_next`, `zat_query_free`
- `zat_pull`, `zat_pull_free`
- Error handling via return codes + `zat_errmsg()`

**Tests:** See 10.17

---

### Step 18: Compression (Page-Level) ⏱️ 3 days

**Files:** `src/page.zig` (extend), `src/encoding.zig` (extend)

Implement within-page compression:
- Delta encoding for entity IDs in EAVT pages
- Dictionary encoding for attribute IDs (small namespace)
- Prefix compression for composite keys with shared prefixes
- Optional zstd per-page compression (for cold data)

**Tests:** Compression roundtrip tests, size comparison benchmarks

---

### Step 19: Bloom Filters ⏱️ 2 days

**Files:** `src/bloom.zig`

Per-subtree bloom filters on leading key components:
- Build filter when a subtree is finalized (all pages written)
- Store filter in branch pages (spare space) or dedicated filter pages
- Check filter before descending into subtree during point lookups
- ~10 bits per key, 1% false positive rate

**Tests:** False positive rate validation, integration with lookup path

---

### Step 20: Benchmarks & Hardening ⏱️ 5 days

**Files:** `tools/zat-bench.zig`, fuzz targets

- Benchmark suite: insert throughput, point lookup, range scan, Datalog queries
- Compare with SQLite for equivalent workloads
- Fuzz testing: random transactions, random queries
- Crash testing: kill process at random points, verify recovery
- Property-based testing: generate random datom sequences, verify index consistency

---

## 10. Test Strategy

### 10.1 Value Encoding Tests 🧪

```
test "encode/decode roundtrip for all value types"
  - For each type: encode → decode → assert equal to original
  - Edge cases: empty string, max i64, min i64, NaN, -0.0, very long strings

test "sortable encoding preserves order"
  - Generate random pairs of each type
  - Assert: compare(a, b) == memcmp(encode(a), encode(b))
  - Specifically for f64: -1.0 < 0.0 < 1.0 < NaN
  - Specifically for i64: MIN < -1 < 0 < 1 < MAX

test "cross-type tag ordering"
  - Encoded nil < encoded bool < encoded i64 < encoded string
```

### 10.2 Page Primitive Tests 🧪

```
test "leaf page insert and lookup"
  - Insert 10 entries, verify all findable by binary search
  - Insert in random order, verify sorted iteration

test "leaf page split"
  - Fill leaf to capacity, trigger split
  - Verify: all entries present across both leaves
  - Verify: left leaf max < right leaf min
  - Verify: linked list pointers updated

test "branch page routing"
  - Create branch with known separators
  - Verify: lookup routes to correct child for keys below/above/between separators

test "overflow page chain"
  - Write 100KB value, verify it spans multiple overflow pages
  - Read back and verify identical
```

### 10.3 File Manager Tests 🧪

```
test "create new database file"
  - Create file, verify meta page 0 written with correct magic/version
  - Verify meta page 1 is zero-initialized
  - Verify file size = 2 * page_size (two meta pages)

test "mmap read after write"
  - Allocate page, write data via pwrite
  - Remap, read via mmap, verify data matches

test "meta page recovery"
  - Write meta page 0 with tx_id=1
  - Write meta page 1 with tx_id=2
  - Reopen, verify tx_id=2 selected
  - Corrupt meta page 1 checksum, reopen, verify tx_id=1 selected

test "page allocation extends file"
  - Allocate N pages, verify file grows by N * page_size
```

### 10.4 B+ Tree Read Tests 🧪

```
test "lookup in single-leaf tree"
  - Build tree with < page capacity entries
  - Lookup each key, verify correct value

test "lookup in multi-level tree"
  - Build tree with 10,000 entries (forces splits)
  - Lookup each key, verify correct value
  - Lookup non-existent key, verify null

test "range scan"
  - Insert keys 1..1000
  - range(100, 200): verify exactly keys 100..200 returned in order

test "iterator forward/backward"
  - Seek to middle of tree
  - next() returns ascending keys
  - prev() returns descending keys

test "empty tree operations"
  - Lookup on empty tree returns null
  - Range scan on empty tree returns empty iterator
```

### 10.5 B+ Tree Write (COW) Tests 🧪

```
test "insert produces new root"
  - Insert into tree, get new_root
  - old_root still points to original tree (COW property)
  - new_root contains inserted key

test "COW preserves old snapshots"
  - Build tree T1 with root R1
  - Insert key K into T1, get root R2
  - Read from R1: K is NOT present
  - Read from R2: K IS present

test "split correctness"
  - Insert enough keys to trigger 3 levels of splits
  - Verify all keys retrievable
  - Verify tree height is correct (log calculation)

test "sequential insert append optimization"
  - Insert keys 1..100000 in order
  - Verify leaf fill factor > 85% (append-mode split)

test "random insert stress test"
  - Insert 100,000 random keys
  - Verify all retrievable
  - Verify tree invariants (sorted, balanced, all leaves same depth)

test "delete and reinsert"
  - Insert keys A..Z, delete M..P, reinsert N
  - Verify exactly the right keys present
```

### 10.6 Free Page Tests 🧪

```
test "freed pages are reused"
  - Insert keys, note page count
  - Delete keys (COW produces old page references)
  - Commit (old pages added to FreeDB)
  - Insert new keys
  - Verify file did NOT grow (pages reused from FreeDB)

test "active reader prevents reclamation"
  - Begin read tx at tx_id=5
  - Commit write tx at tx_id=6 (frees some pages)
  - Attempt alloc: should NOT reuse freed pages (reader at 5)
  - End read tx
  - Attempt alloc: SHOULD reuse freed pages
```

### 10.7 Schema Tests 🧪

```
test "bootstrap creates meta-schema"
  - Create new database
  - Verify :db/ident, :db/valueType, etc. are queryable
  - Verify schema cache populated with all bootstrap attributes

test "user schema definition"
  - Transact: {:db/ident :user/name, :db/valueType :db.type/string, :db/cardinality :db.cardinality/one}
  - Verify schema cache updated
  - Verify attribute usable in subsequent transactions

test "schema validation rejects bad types"
  - Define :user/age as :db.type/i64
  - Attempt transact: [:db/add 1 :user/age "not a number"]
  - Verify: error returned, database unchanged
```

### 10.8 Index Manager Tests 🧪

```
test "datom appears in correct indexes"
  - Insert datom with indexed ref attribute
  - Verify present in EAV, AVE, VAE, TxLog (all four)
  - Insert datom with non-indexed, non-ref attribute
  - Verify present in EAV, TxLog only (not AVE, not VAE)

test "EAV lookup by entity"
  - Insert multiple datoms for entity 42
  - EAV seek(42) returns all attributes of entity 42

test "AVE lookup by value"
  - Insert datom [:user/email "alice@example.com"] (indexed)
  - AVE seek(:user/email, "alice@example.com") returns entity ID

test "VAE reverse reference"
  - Insert [entity-1 :user/friend entity-2] (ref type)
  - VAE seek(entity-2, :user/friend) returns entity-1
```

### 10.9 Transaction Tests 🧪

```
test "basic assert and retract"
  - Transact: assert [1 :user/name "Alice"]
  - Verify retrievable
  - Transact: retract [1 :user/name "Alice"]
  - Verify NOT retrievable in current db
  - Verify still in TxLog (history)

test "tempid resolution"
  - Transact with tempids: ["user-1" :user/name "Alice"], ["user-1" :user/age 30]
  - Verify both datoms have same entity ID in tx report

test "cardinality one replacement"
  - Define :user/name as cardinality one
  - Transact: [1 :user/name "Alice"]
  - Transact: [1 :user/name "Bob"]
  - Verify: entity 1 has name "Bob", NOT "Alice"
  - Verify: retraction of "Alice" in TxLog

test "unique identity upsert"
  - Define :user/email as :db.unique/identity
  - Transact: [tempid :user/email "a@b.com"] [tempid :user/name "Alice"]
  - Transact: [tempid :user/email "a@b.com"] [tempid :user/name "Alicia"]
  - Verify: same entity ID, name updated to "Alicia"

test "unique value conflict"
  - Define :user/ssn as :db.unique/value
  - Transact: [1 :user/ssn "123"]
  - Transact: [2 :user/ssn "123"]
  - Verify: error, transaction rejected

test "transaction entity metadata"
  - Transact with tx metadata: [:db/tx :audit/user "admin"]
  - Verify: tx entity has :db/txInstant and :audit/user

test "crash recovery: committed tx survives"
  - Transact data
  - Simulate crash (close without clean shutdown)
  - Reopen database
  - Verify data present

test "crash recovery: uncommitted tx lost"
  - Begin transaction, write pages, do NOT write meta page
  - Simulate crash
  - Reopen database
  - Verify data NOT present
```

### 10.10 Datalog Parser Tests 🧪

```
test "parse simple find-where"
  - Input: "[:find ?name :where [?e :user/name ?name]]"
  - Verify AST: 1 find var, 1 pattern clause

test "parse with predicates"
  - Input: "[:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age] [(> ?age 30)]]"
  - Verify: 2 pattern clauses + 1 predicate clause

test "parse with :in clause"
  - Input: "[:find ?name :in $ ?min-age :where [?e :user/name ?name] [?e :user/age ?age] [(>= ?age ?min-age)]]"
  - Verify: 1 input var (?min-age)

test "parse error on malformed input"
  - Missing :find → error
  - Unclosed bracket → error
  - Unknown keyword → error
```

### 10.11 Query Executor Tests 🧪

```
test "simple entity lookup"
  - Insert: [1 :user/name "Alice"], [2 :user/name "Bob"]
  - Query: [:find ?name :where [?e :user/name ?name]]
  - Verify: {"Alice", "Bob"}

test "two-clause join"
  - Insert: [1 :user/name "Alice"], [1 :user/age 30], [2 :user/name "Bob"], [2 :user/age 25]
  - Query: [:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age]]
  - Verify: {("Alice", 30), ("Bob", 25)}

test "predicate filtering"
  - Same data as above
  - Query: [:find ?name :where [?e :user/name ?name] [?e :user/age ?age] [(> ?age 28)]]
  - Verify: {"Alice"}

test "reverse reference traversal"
  - Insert: [1 :user/name "Alice"], [2 :user/name "Bob"], [1 :user/friend 2]
  - Query: [:find ?friend-name :where [1 :user/friend ?f] [?f :user/name ?friend-name]]
  - Verify: {"Bob"}

test "query with constant entity"
  - Query: [:find ?name :where [42 :user/name ?name]]
  - Verify uses EAV index directly

test "query returns no duplicates"
  - Insert data where join would produce dupes without :find dedup
  - Verify result set has no duplicates
```

### 10.12 Time Travel Tests 🧪

```
test "as-of sees past state"
  - tx1: [1 :user/name "Alice"]
  - tx2: [1 :user/name "Alicia"]  (cardinality/one replacement)
  - db.asOf(tx1): entity 1 name = "Alice"
  - db.asOf(tx2): entity 1 name = "Alicia"
  - db (current): entity 1 name = "Alicia"

test "since shows only recent changes"
  - tx1: [1 :user/name "Alice"]
  - tx2: [2 :user/name "Bob"]
  - db.since(tx1): only entity 2 visible

test "history shows all datoms including retractions"
  - tx1: assert [1 :user/name "Alice"]
  - tx2: retract [1 :user/name "Alice"], assert [1 :user/name "Alicia"]
  - db.history() query for entity 1 :user/name:
    returns both "Alice" (op=true, tx1), "Alice" (op=false, tx2), "Alicia" (op=true, tx2)
```

### 10.13 Concurrency Tests 🧪

```
test "concurrent reads don't block"
  - Spawn N reader threads, each doing lookups
  - Verify all complete without deadlock

test "writer doesn't block readers"
  - Start long-running read transaction
  - Start write transaction on another thread
  - Verify write completes while read is active
  - Verify read still sees pre-write state

test "two writers serialize"
  - Start two write transactions on different threads
  - Verify one blocks until the other commits

test "stale reader detection"
  - Open read tx, record PID, simulate process death
  - New writer should detect stale reader and reclaim slot
```

### 10.14 Query Optimizer Tests 🧪

```
test "clause reordering puts selective clauses first"
  - Schema: :user/email is unique, :user/age is not
  - Query with clauses in order: [?e :user/age ?age] [?e :user/email "x@y.com"]
  - Verify optimizer reorders: email clause first (cardinality 1) then age

test "bound variable triggers index lookup instead of scan"
  - Two clauses sharing ?e
  - Verify second clause uses EAV point lookup, not full scan
```

### 10.15 Leapfrog TrieJoin Tests 🧪

```
test "triangle query"
  - Insert friendship graph: A→B, B→C, A→C
  - Query: [:find ?a ?b ?c :where [?a :friend ?b] [?b :friend ?c] [?a :friend ?c]]
  - Verify: finds the triangle

test "leapfrog matches nested loop results"
  - Generate random graph with 1000 nodes
  - Run same query with leapfrog and nested-loop
  - Verify identical results
```

### 10.16 Pull API Tests 🧪

```
test "pull flat attributes"
  - pull(entity, [:user/name :user/age])
  - Verify: { :user/name "Alice", :user/age 30 }

test "pull with nested join"
  - pull(entity, [:user/name {:user/friends [:user/name]}])
  - Verify: nested structure with friend names

test "pull missing attribute returns nil"
  - pull(entity, [:user/phone])
  - Verify: empty/nil for missing attr
```

### 10.17 C API Tests 🧪

```
test "C API lifecycle"
  - Call zat_open, zat_tx_begin, zat_tx_add, zat_tx_commit, zat_query, zat_close
  - From a C test file compiled separately
  - Verify correct results through C interface

test "C API error handling"
  - Invalid file path → error code
  - Schema violation → error code + message via zat_errmsg()
```

---

## Appendix A: Estimated Timeline

| Step | Description                  | Days | Cumulative | Milestone                      |
|------|------------------------------|------|------------|--------------------------------|
| 0    | Project scaffolding          | 1    | 1          |                                |
| 1    | Value encoding               | 2    | 3          |                                |
| 2    | Page primitives              | 2    | 5          |                                |
| 3    | File manager + mmap          | 3    | 8          |                                |
| 4    | B+ tree read path            | 3    | 11         |                                |
| 5    | B+ tree write (COW)          | 5    | 16         | Can store/retrieve key-values  |
| 6    | Free page tracking           | 2    | 18         |                                |
| 7    | Schema & bootstrap           | 2    | 20         |                                |
| 8    | Index manager                | 3    | 23         |                                |
| 9    | Transaction processor        | 4    | 27         | **🎉 Working database (MVP)**  |
| 10   | Datalog parser               | 3    | 30         |                                |
| 11   | Basic query executor         | 4    | 34         | **🎉 Can run Datalog queries** |
| 12   | Time travel                  | 3    | 37         |                                |
| 13   | Concurrent readers           | 3    | 40         |                                |
| 14   | Query optimizer              | 3    | 43         |                                |
| 15   | Leapfrog TrieJoin            | 4    | 47         | **🎉 Full query engine**       |
| 16   | Pull API                     | 2    | 49         |                                |
| 17   | C API                        | 3    | 52         | **🎉 Embeddable library**      |
| 18   | Compression                  | 3    | 55         |                                |
| 19   | Bloom filters                | 2    | 57         |                                |
| 20   | Benchmarks & hardening       | 5    | 62         | **🎉 Production-ready v0.1**   |

**Total: ~62 person-days (~3 months at sustainable pace)**

---

## Appendix B: Key Data Structure Sizes

For capacity planning and page layout math:

```
Datom (in memory):        ~40 bytes (E:8 + A:8 + V:8-ptr + Tx:8 + Op:1 + padding)
Datom (encoded, avg):     ~25 bytes (E:8 + A:8 + V-tag:1 + V-payload:~6 + Op:1)
Datom (with DUPSORT):     ~17 bytes avg (E:0-amortized + A:8 + V:~8 + Op:1)

Leaf page capacity (4KB): ~150 datoms (DUPSORT)
Leaf page capacity (16KB): ~600 datoms (DUPSORT)

Branch page capacity (4KB): ~200 entries (8-byte keys + 8-byte pointers)
Branch page capacity (16KB): ~800 entries

Tree height for 1M datoms (4KB pages):
  Leaves: ~6,700 pages
  Level 1: ~34 branch pages
  Level 2: 1 root page
  Total: 3 levels

Tree height for 100M datoms (4KB pages):
  Leaves: ~670,000 pages
  Level 1: ~3,350 pages
  Level 2: ~17 pages
  Level 3: 1 root page
  Total: 4 levels
```

---

*Document version: 0.2 — Technical implementation design*
*Date: 2026-02-21*
*Previous: v0.1 (initial design draft)*
