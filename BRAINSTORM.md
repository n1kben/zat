# Brainstorm: Persistent Data Structures for ZatDB

**Date:** 2026-02-22
**Context:** Steps 0-9 complete (~7K lines). RFC-001 proposes persistent architecture.
This document captures the full brainstorm session: analysis, problems found,
ideas explored, and conclusions reached.

---

## Part 1: RFC-001 vs. Reality

### What the RFC proposes

Replace ZatDB's LMDB-style COW B+ tree (dual meta pages, free page tracking)
with a fully persistent append-only architecture:

- Pages never freed or overwritten
- Every transaction's roots preserved in a root log
- Time travel = pick a different root, O(1)
- Remove B+ tree delete, free page tracking, dual meta pages

### Critical finding: the RFC was written against the spec, not the code

The RFC assumes DUPSORT-style encoding from IMPLEMENTATION.md:

```
EAV Key:   [E: 8 bytes]
EAV Value: [A: 8 bytes][value_tag][value_payload]
```

The actual code uses flat composite keys with empty values:

```
EAV Key:   [E:8][A:8][encoded_value:var]   value = &.{} (empty)
AVE Key:   [A:8][encoded_value:var][E:8]    value = &.{} (empty)
VAE Key:   [V:8][A:8][E:8]                  value = &.{} (empty)
TxLog Key: [Tx:8][E:8][A:8][encoded_value:var][Op:1]  value = &.{}
```

This matters because:

- **`put()` as the RFC describes it is impossible.** All data is in the key.
  For cardinality-one, old key `[E][A][old_V]` and new key `[E][A][new_V]`
  are different byte strings. You must delete one and insert another.
- **`removeDup()` is impossible.** No DUPSORT groups exist.
- **B+ tree `delete()` cannot be removed** with this encoding.

### RFC problem statements: what's accurate, what's overstated

| RFC Claim | Verdict | Detail |
|-----------|---------|--------|
| "B+ tree delete with merge-on-underflow is complex" | **Overstated** | `delete()` is ~80 lines with NO merge-on-underflow. Already the simplest possible COW delete. |
| "FreeDB is complex and self-referential" | **Accurate** | `freelist.zig` is 442 lines. Has genuine self-referential complexity (`carry_forward` tracks FreeDB's own COW pages during insertion into itself). |
| "Dual meta pages only remember two versions" | **Accurate** | `pickActiveMeta()` selects higher `tx_id`, previous state overwritten on next commit. |
| "Time travel requires TxLog replay" | **Accurate** | EAV/AVE/VAE store current state only. `asOf()` would need TxLog scanning. |
| "Two different datom encodings" | **Accurate** | TxLog includes Tx+Op; EAV/AVE/VAE do not. |

### The `fixLeafSiblings` bug

`btree.zig:448-487` — When a leaf is COW'd, its neighbors' `prev_leaf`/`next_leaf`
pointers are **mutated in-place** at their original page numbers:

```zig
// Writes directly to the OLD page at its original location
try self.fm.writePage(prev_id, b);
```

Old roots referencing those neighbor pages find their sibling pointers pointing
to pages from a future transaction. Range scans via old roots follow corrupted
sibling chains. This is a **pre-existing bug** that the root log feature would
immediately surface.

---

## Part 2: What Does "Persistent" Actually Require?

The RFC conflated "no delete" with "persistent." They're different things.

A persistent data structure requires three invariants:

1. **No page is ever mutated in place** — every write creates new pages
2. **No page is ever freed** — old pages remain accessible forever
3. **Every root is preserved** — you can reach any historical state

If all three hold, old roots remain valid. **COW delete is fine** — it creates
new pages via path-copy. The old leaf (with the entry) still lives at its
original page number, reachable from old roots.

### What currently violates these invariants

| Invariant | Violation | Where |
|-----------|-----------|-------|
| No mutation | `fixLeafSiblings` writes prev/next pointers in-place | `btree.zig:448-487` |
| No freeing | FreeDB reclaims old COW'd pages | `freelist.zig` |
| Roots preserved | Dual meta keeps only 2 roots | `meta.zig` |

Fix these three things and you have a persistent data structure. The key
encoding doesn't need to change. `delete()` doesn't need to be removed.

### The sibling pointer problem

`fixLeafSiblings` is the only place in the entire codebase that mutates an
existing page. Three options were considered:

**A. COW sibling pages too.** When you COW a leaf, also COW its neighbors to
update their pointers, then propagate those up. Cascading COW. Expensive: a
single leaf split could COW 3 leaves + up to 6 ancestor branches.

**B. Remove sibling pointers entirely. Use stack-based iteration.** The iterator
remembers its path from root to leaf. When a leaf is exhausted, ascend to
parent, advance to next child, descend to next leaf. O(tree_height) per leaf
boundary, O(1) amortized over full scan. Stack is ~40 bytes. This is what
persistent B+ trees in FP literature use (Okasaki, Hinze/Paterson).

**C. Keep only forward pointers (singly-linked).** Half-measure, still has
mutation.

**Conclusion: Option B is the right answer.** It makes leaves truly independent,
removes all in-place mutation, and simplifies the page format (remove 16 bytes
of prev_leaf/next_leaf).

---

## Part 3: The Encoding Insight

### The question that changed everything

> "Shouldn't the key be 42, then a sub tree with name and a subtree with
> values that changed over time?"

This points to a fundamental problem with the current encoding.

### Current encoding: everything in the key

```
EAV B+ Tree
────────────────────────────────────
Key                           Value
────────────────────────────────────
[42][:age]   [30]             (empty)
[42][:email] ["a@b.com"]      (empty)
[42][:name]  ["Alice"]        (empty)
────────────────────────────────────
```

Changing name from "Alice" to "Bob" means the KEY changes. Old key
`[42][:name]["Alice"]` and new key `[42][:name]["Bob"]` are different byte
strings. Requires delete + insert. Two COW operations per update.

### Better encoding: key = [E][A], value = [V]

```
EAV B+ Tree
────────────────────────────────────
Key              Value
────────────────────────────────────
[42][:age]       [30]
[42][:email]     ["a@b.com"]
[42][:name]      ["Alice"]    ← just replace this value
────────────────────────────────────
```

Changing "Alice" to "Bob" is `put(key=[42][:name], value="Bob")`. Same key,
new value. COW the leaf, overwrite the value slot in the copy. One COW
operation. No delete.

### How this looks with persistence

```
       Root 1 (tx 1)                 Root 2 (tx 2)
       ┌──────────┐                 ┌──────────┐
       │ ptr: 20  │                 │ ptr: 21  │
       └──────────┘                 └──────────┘
            │                            │
            ▼                            ▼
       Leaf (page 20)               Leaf (page 21)   ← COW copy
      ┌──────────────────┐        ┌──────────────────┐
      │[42][:age]  = 30  │        │[42][:age]  = 30  │
      │[42][:email]= a@b │        │[42][:email]= a@b │
      │[42][:name] = Alice│       │[42][:name] = Bob │ ← value replaced
      └──────────────────┘        └──────────────────┘
         still exists!               new page
         root 1 → page 20           root 2 → page 21
         time travel works           latest state
```

Page 20 is never touched. Root 1 still sees "Alice." Root 2 sees "Bob."
Structural sharing: most of the tree is identical between transactions.

### What about cardinality-many?

Entity 42 has multiple friends. Key `[42][:friends]` maps to... what? Can't
store multiple values in one slot.

**Option A: Encode cardinality into the key for the many case.**

```
Cardinality-one:   key=[E][A]      value=[V]      → put() replaces
Cardinality-many:  key=[E][A][V]   value=(empty)  → insert/delete
```

Cardinality-many still needs delete, but it's the uncommon case and the delete
is simple COW (no merge, no free pages).

**Option B: Store a sorted list as the value.**

```
Key              Value
────────────────────────────────────
[42][:friends]   [ref:43, ref:44, ref:45]
```

Adding friend 46 = read old list, append, sort, `put()` the new list. No delete
ever. But large sets make the value huge.

**Option A is the practical choice.** It's what real databases do.

### Impact on secondary indexes

**AVE index** (value lookups: "find entity where name = Alice"):

- Key must include the value for lookup: key=[A][V], value=[E]
- Changing "Alice" to "Bob" means the AVE key changes
- Still needs delete in AVE (remove [A]["Alice"][42], insert [A]["Bob"][42])
- But AVE is a secondary index, only for `:db/index true` attributes

**VAE index** (reverse ref traversal):

- Key=[V(ref)][A], value=[E]
- Changing a ref attribute = AVE key changes, needs delete
- Only for `:db.type/ref` attributes

**Summary:**

| Index | Encoding | Cardinality-one update | Delete needed? |
|-------|----------|----------------------|----------------|
| EAV | key=[E][A], value=[V] | `put()` — replace value | **No** (except retractions) |
| AVE | key=[A][V], value=[E] | delete old + insert new | Yes |
| VAE | key=[V][A], value=[E] | delete old + insert new | Yes |

The primary index (EAV) becomes delete-free for the common case.
Secondary indexes still need delete, but they're simpler and less critical.

---

## Part 4: Conclusions and Plan

### What we now believe

1. **The RFC's core goal is right** — persistent data structures with fast time
   travel. But the RFC's proposed mechanism (remove delete, DUPSORT encoding)
   doesn't work with the actual codebase and isn't necessary.

2. **Persistence requires three things:** no mutation, no freeing, all roots
   preserved. COW delete is compatible with persistence.

3. **The encoding change from `[E][A][V]→empty` to `[E][A]→[V]`** eliminates
   delete from the primary index (EAV) for cardinality-one, which is the vast
   majority of updates. This is the single change that makes the architecture
   click.

4. **Sibling pointers must go.** They're the only source of in-place mutation.
   Stack-based iteration is the clean replacement.

5. **FreeDB must go.** Pages are never freed in a persistent architecture.

6. **Dual meta must be replaced with a root log.** Every historical root
   preserved. Time travel = root log lookup.

### The phases

**Phase 1: Remove FreeDB** (pure subtraction, ~half day)

- Delete `freelist.zig` entirely (-442 lines)
- Remove FreeDB wiring from `tx.zig`, `file.zig`, `btree.zig`
- Simplify `allocPage()` to monotonic counter
- All existing tests should still pass

**Phase 2: Stack-based iteration, remove sibling pointers** (~1-2 days)

- Remove `prev_leaf`/`next_leaf` from leaf page format
- Delete `fixLeafSiblings` entirely
- Rewrite Iterator to use parent stack
- No page is ever mutated after this phase

**Phase 3: Re-encode EAV as key=[E][A], value=[V]** (~1-2 days)

- Change `schema.zig` encoding functions
- Change `index.zig` routing
- Add `put()` to B+ tree (COW copy leaf, replace value at key)
- Cardinality-one uses `put()`, cardinality-many uses `insert()`/`delete()`
- Update comparison functions
- AVE/VAE keep secondary-index encoding with delete

**Phase 4: Root log** (~1-2 days)

- Replace dual meta with append-only root log
- Each commit appends `{tx_id, eav_root, ave_root, vae_root, ...}`
- `asOf(tx_id)` = root log lookup + tree init with old roots
- Bump file format version to 2

**Phase 5: Time travel API** (~1 day)

- `asOf(tx_id)` — returns DatabaseValue with historical roots
- `history(E, A)` — scan TxLog for all assertions/retractions
- `since(tx_id)` — diff two database values (merge-join of two EAV iterators)

### What we're NOT doing

- Not removing `delete()` from B+ tree — still needed for AVE/VAE and
  cardinality-many and explicit retractions
- Not implementing merge-on-underflow — pages may become sparse, that's fine
- Not converting TxLog to append-only chain — keep B+ tree for random access
- Not implementing compaction yet — but the architecture makes it possible
  (copy reachable pages from selected roots to new file)

---

## Open Questions

### Q1: Cardinality-many encoding

Option A (key=[E][A][V], value=empty) needs delete for removal. Option B
(value is a sorted list) avoids delete but makes large sets expensive.
Is there a hybrid? E.g., small sets (< 50 values) as list in value,
large sets spill to sub-tree?

### Q2: AVE encoding with new EAV format

With EAV as key=[E][A] value=[V], the AVE index needs key=[A][V] value=[E].
But we said V is now in the EAV value, not the key. The AVE tree is a
separate tree with its own encoding — that's fine. But it means the same
value is encoded differently in EAV vs AVE. Worth thinking about whether
this creates complexity in the index manager.

### Q3: File growth

The file grows forever. Rough math: each cardinality-one update on a
3-level tree generates ~3 new pages (leaf + 2 branches). At 16KB pages,
that's 48KB per update. At 1 update/sec: ~4 GB/day. At 100 updates/sec:
~400 GB/day. Compaction is essential for production use but can be deferred.

### Q4: Root log scan performance

With 76-byte root records and 4KB pages, each page holds ~53 entries.
After 1M transactions: ~19K root log pages. `asOf(tx_id=5)` from the
tail is O(19K) page reads. Solutions:

- Binary search (root log pages are sorted by tx_id)
- B+ tree root log instead of linked list
- Keep it simple for now, optimize if it matters

### Q5: mmap growth strategy

When file grows, mmap must be extended. Options:

- `mremap()` on Linux (fast, atomic)
- `munmap()` + `mmap()` on macOS (brief invalid window)
- Over-map: mmap a large virtual region (1TB), file grows into it

Over-map with `MAP_NORESERVE` is probably the answer for both platforms.

### Q6: put() when value size changes

If the new value is larger than the old value in a leaf entry, the entry
grows. This might cause the leaf to overflow, requiring a split. `put()`
needs to handle this: replace value, check if leaf overflows, split if
needed. Same as insert but with a replacement instead of addition.

If the new value is smaller, the leaf has wasted space. This is fine —
no compaction within pages, just slightly lower fill factor.

### Q7: What about the original IMPLEMENTATION.md DUPSORT design?

The spec describes DUPSORT encoding but the code implements flat keys.
Should we align with the spec (implement DUPSORT) or align the spec with
the code? The [E][A]→[V] encoding is simpler than full DUPSORT and gets
us most of the benefit. Update the spec to match.
