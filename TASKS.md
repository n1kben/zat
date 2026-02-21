# ZatDB Tasks

Ordered implementation tasks. Each maps to a step in IMPLEMENTATION.md.

Status legend: `[ ]` pending, `[~]` in progress, `[x]` complete

---

## Task 1: Project Scaffolding (Step 0)
- Status: [x] complete
- Deps: none
- Files: build.zig, build.zig.zon, src/main.zig
- Done when: `zig build test` passes with placeholder test

## Task 2: Value Encoding (Step 1)
- Status: [ ] pending
- Deps: Task 1
- Files: src/encoding.zig
- Done when: roundtrip encode/decode tests pass for all value types (nil, bool, i64, f64, string, keyword, ref, instant, uuid, bytes); sortable encoding preserves order; cross-type tag ordering correct
- Ref: IMPLEMENTATION.md Section 2.2, Tests 10.1

## Task 3: Page Primitives (Step 2)
- Status: [ ] pending
- Deps: Task 2
- Files: src/page.zig
- Done when: leaf page insert/lookup/split works; branch page routing works; overflow page chain read/write works
- Ref: IMPLEMENTATION.md Sections 3.3-3.6, Tests 10.2

## Task 4: File Manager & mmap (Step 3)
- Status: [ ] pending
- Deps: Task 3
- Files: src/file.zig, src/meta.zig
- Done when: can create new .zat file with valid meta page; mmap read after write works; meta page recovery (checksum validation, pick higher tx_id); page allocation extends file
- Ref: IMPLEMENTATION.md Section 3.1-3.2, Tests 10.3

## Task 5: B+ Tree Read Path (Step 4)
- Status: [ ] pending
- Deps: Task 3, Task 4
- Files: src/btree.zig
- Done when: lookup in single-leaf and multi-level trees works; range scan returns correct ordered results; forward/backward iteration works; empty tree returns null/empty
- Ref: IMPLEMENTATION.md Section 4.1 (Lookup, Range Scan), Tests 10.4

## Task 6: B+ Tree Write Path — COW (Step 5)
- Status: [ ] pending
- Deps: Task 5
- Files: src/btree.zig (extend)
- Done when: insert produces new root (COW); old snapshots preserved; splits correct at 3+ levels; sequential insert append optimization (>85% fill); random insert stress test (100K keys); delete and reinsert works
- Ref: IMPLEMENTATION.md Section 4.1-4.3, Tests 10.5

## Task 7: Free Page Tracking (Step 6)
- Status: [ ] pending
- Deps: Task 6
- Files: src/btree.zig (extend), src/file.zig (extend)
- Done when: freed pages are reused (file doesn't grow); active reader prevents reclamation; FreeDB B+ tree keyed by tx_id works
- Ref: IMPLEMENTATION.md Section 4.2, Tests 10.6

## Task 8: Schema & Bootstrap (Step 7)
- Status: [ ] pending
- Deps: Task 6
- Files: src/schema.zig
- Done when: bootstrap creates meta-schema (:db/ident, :db/valueType, etc.); user schema definition works; schema validation rejects bad types
- Ref: IMPLEMENTATION.md Sections 5.3-5.4, Tests 10.7

## Task 9: Index Manager (Step 8)
- Status: [ ] pending
- Deps: Task 6, Task 8
- Files: src/index.zig
- Done when: datom appears in correct indexes (EAV+TxLog always, AVE if indexed, VAE if ref); EAV lookup by entity works; AVE lookup by value works; VAE reverse reference works
- Ref: IMPLEMENTATION.md Section 5.1-5.2, Tests 10.8

## Task 10: Transaction Processor (Step 9)
- Status: [ ] pending
- Deps: Task 9
- Files: src/tx.zig
- Done when: basic assert/retract works; tempid resolution works; cardinality-one replacement works; unique identity upsert works; unique value conflict rejected; tx entity metadata stored; crash recovery (committed survives, uncommitted lost)
- Ref: IMPLEMENTATION.md Section 6, Tests 10.9
- NOTE: This is the first major milestone — working embedded DB after this task

## Task 11: Datalog Parser (Step 10)
- Status: [ ] pending
- Deps: Task 2
- Files: src/query/parser.zig
- Done when: parses simple find-where queries; parses predicates [(> ?age 30)]; parses :in clause; error on malformed input (missing :find, unclosed bracket)
- Ref: IMPLEMENTATION.md Section 7.1-7.2, Tests 10.10

## Task 12: Basic Query Executor (Step 11)
- Status: [ ] pending
- Deps: Task 10, Task 11
- Files: src/query/executor.zig, src/query/planner.zig
- Done when: simple entity lookup works; two-clause join works; predicate filtering works; reverse reference traversal works; query with constant entity uses EAV directly; no duplicates in results
- Ref: IMPLEMENTATION.md Section 7.3 (naive nested-loop), Tests 10.11

## Task 13: Time Travel (Step 12)
- Status: [ ] pending
- Deps: Task 12
- Files: src/query/executor.zig (extend), src/index.zig (extend)
- Done when: asOf sees past state; since shows only recent changes; history shows all datoms including retractions
- Ref: IMPLEMENTATION.md Section 7.5, Tests 10.12

## Task 14: Concurrent Readers (Step 13)
- Status: [ ] pending
- Deps: Task 10
- Files: src/main.zig (extend), src/file.zig (extend)
- Done when: concurrent reads don't block; writer doesn't block readers; two writers serialize; stale reader detection works
- Ref: IMPLEMENTATION.md Section 8, Tests 10.13

## Task 15: Query Optimizer (Step 14)
- Status: [ ] pending
- Deps: Task 12
- Files: src/query/planner.zig (extend)
- Done when: clause reordering puts selective clauses first; bound variable triggers index lookup instead of scan
- Ref: IMPLEMENTATION.md Section 7.3, Tests 10.14

## Task 16: Leapfrog TrieJoin (Step 15)
- Status: [ ] pending
- Deps: Task 15
- Files: src/query/leapfrog.zig
- Done when: triangle query works; leapfrog matches nested-loop results on random graph
- Ref: IMPLEMENTATION.md Section 7.4, Tests 10.15

## Task 17: Pull API (Step 16)
- Status: [ ] pending
- Deps: Task 12
- Files: src/query/pull.zig
- Done when: pull flat attributes works; pull with nested join works; pull missing attribute returns nil
- Ref: IMPLEMENTATION.md Section 7.6, Tests 10.16

## Task 18: C API (Step 17)
- Status: [ ] pending
- Deps: Task 10, Task 12
- Files: src/c_api.zig
- Done when: C API lifecycle (open/tx/query/close) works; error handling returns codes + messages
- Ref: IMPLEMENTATION.md Step 17, Tests 10.17

## Task 19: Page Compression (Step 18)
- Status: [ ] pending
- Deps: Task 10
- Files: src/page.zig (extend), src/encoding.zig (extend)
- Done when: delta encoding for entity IDs; dictionary encoding for attribute IDs; prefix compression for composite keys; compression roundtrip tests pass
- Ref: IMPLEMENTATION.md Step 18

## Task 20: Benchmarks & Hardening (Step 20)
- Status: [ ] pending
- Deps: Task 18
- Files: tools/zat-bench.zig, fuzz targets
- Done when: benchmark suite runs (insert throughput, point lookup, range scan, Datalog queries); fuzz testing finds no crashes; crash testing verifies recovery
- Ref: IMPLEMENTATION.md Step 20
