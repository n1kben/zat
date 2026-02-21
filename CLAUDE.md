# ZatDB

Datomic-style embedded database in Zig. Single `.zat` file. Zero external dependencies.

## Architecture

- **Storage**: LMDB-style copy-on-write B+ tree. No WAL — dual meta pages for crash safety.
- **Access**: mmap for zero-copy reads, pwrite for writes.
- **Indexes**: EAV (all datoms), AVE (indexed attrs), VAE (ref attrs), TxLog (all datoms by tx).
- **DUPSORT**: Within each B+ tree key, multiple sorted values (e.g., one entity key → many attribute-value pairs).
- **Concurrency**: Single writer, multiple readers via reader slot table.
- **Queries**: Datalog with hand-written parser, cost-based planner, LeapfrogTrieJoin for cyclic joins.

## File Structure (Target)

```
src/
  main.zig          Library root, public API
  encoding.zig      Value encoding/decoding (sortable binary format)
  page.zig          Page types, header, leaf/branch/overflow serialization
  btree.zig         COW B+ tree core (read + write paths)
  meta.zig          Meta page read/write/checksum validation
  file.zig          File I/O, mmap management, page allocation
  index.zig         Index manager (EAV, AVE, VAE, TxLog, FreeDB)
  tx.zig            Transaction processor (tempids, schema validation, commit)
  schema.zig        Schema cache, bootstrap transaction
  query/
    parser.zig      Datalog parser (hand-written recursive descent)
    planner.zig     Query planner (cost-based clause reordering)
    executor.zig    Execution engine (nested-loop join)
    leapfrog.zig    LeapfrogTrieJoin (WCOJ for cyclic queries)
  c_api.zig         C FFI bindings
```

## Zig Conventions

- Zig 0.15+, zero external dependencies
- Big-endian keys for bytewise sortable comparison
- Zero-copy reads via mmap pointer arithmetic
- All integers stored big-endian in encoded keys/values
- Sortable float encoding: flip sign bit (positive) or invert all bits (negative)
- Sortable i64 encoding: flip sign bit for unsigned comparison
- Page size: auto-detect OS page size (16384 on Apple Silicon)

## Workflow

1. Read TASKS.md to see all tasks and their status.
2. Read progress.txt to understand what's been done in previous sessions.
3. Pick the next unfinished task (respect dependencies — don't skip ahead).
4. Read the relevant section of IMPLEMENTATION.md for detailed specs.
5. Implement the task. Write clean, idiomatic Zig code.
6. Run `zig build` — fix any compilation errors.
7. Run `zig build test` — fix any test failures.
8. Update the task status in TASKS.md to `[x]`.
9. Append progress to progress.txt (task, files changed, test results, decisions).
10. Git commit with a descriptive conventional commit message.

## Feedback Loops

- `zig build` must compile with no errors.
- `zig build test` must pass all tests (existing + new).
- Each task has acceptance criteria — check them before marking complete.

## Commit Style

Conventional commits. One feature per commit. Examples:
- `feat(encoding): implement value encode/decode with sortable binary format`
- `feat(page): add leaf and branch page primitives`
- `fix(btree): correct off-by-one in leaf split`
- `test(tx): add cardinality-one replacement test`

## Key References

- `IMPLEMENTATION.md` — Full technical design (20 steps, test strategy, data layouts)
- `TASKS.md` — Ordered task list with status and acceptance criteria
- `progress.txt` — Cross-session memory of what's been done
