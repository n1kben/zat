// ZatDB — B+ tree read path
//
// Read-only B+ tree operations: point lookups, bidirectional iteration,
// and range scans. Operates on pages via FileManager (mmap-backed reads).
//
// BPlusTree is cheap to copy (root page ID + pointer to FileManager + cmp fn).
// Iterator stores leaf_buf (mmap slice) directly — zero-copy, no allocations.

const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;
const page = @import("page.zig");
const file = @import("file.zig");

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub const BPlusTree = struct {
    root: u64,
    fm: *const file.FileManager,
    cmp: page.KeyCompareFn,

    pub fn init(root_page: u64, fm: *const file.FileManager, cmp: page.KeyCompareFn) BPlusTree {
        return .{ .root = root_page, .fm = fm, .cmp = cmp };
    }

    pub fn lookup(self: BPlusTree, key: []const u8) ?[]const u8 {
        if (self.root == 0) return null;
        const leaf_buf = self.findLeaf(key);
        const slot = page.leafFindKey(leaf_buf, key, self.cmp) orelse return null;
        return page.leafGetValue(leaf_buf, slot);
    }

    pub fn seek(self: BPlusTree, key: []const u8) Iterator {
        if (self.root == 0) return self.emptyIterator();
        const leaf_buf = self.findLeaf(key);
        const count = page.leafEntryCount(leaf_buf);
        const slot = page.leafSearchPoint(leaf_buf, key, self.cmp);
        if (slot >= count) {
            const next_id = page.getLeafNextPage(leaf_buf);
            if (next_id == 0) {
                // Past all entries — record leaf position for prev()
                return .{
                    .tree = self,
                    .leaf_buf = leaf_buf,
                    .slot = count,
                    .entry_count = count,
                    .end_key = null,
                    .exhausted = true,
                };
            }
            const next_buf = self.fm.readPage(next_id);
            return .{
                .tree = self,
                .leaf_buf = next_buf,
                .slot = 0,
                .entry_count = page.leafEntryCount(next_buf),
                .end_key = null,
                .exhausted = false,
            };
        }
        return .{
            .tree = self,
            .leaf_buf = leaf_buf,
            .slot = slot,
            .entry_count = count,
            .end_key = null,
            .exhausted = false,
        };
    }

    pub fn seekFirst(self: BPlusTree) Iterator {
        if (self.root == 0) return self.emptyIterator();
        const leaf_buf = self.findLeftmostLeaf();
        return .{
            .tree = self,
            .leaf_buf = leaf_buf,
            .slot = 0,
            .entry_count = page.leafEntryCount(leaf_buf),
            .end_key = null,
            .exhausted = false,
        };
    }

    pub fn seekLast(self: BPlusTree) Iterator {
        if (self.root == 0) return self.emptyIterator();
        const leaf_buf = self.findRightmostLeaf();
        const count = page.leafEntryCount(leaf_buf);
        return .{
            .tree = self,
            .leaf_buf = leaf_buf,
            .slot = count,
            .entry_count = count,
            .end_key = null,
            .exhausted = true,
        };
    }

    pub fn range(self: BPlusTree, start: []const u8, end_key: []const u8) Iterator {
        if (self.root == 0) return self.emptyIterator();
        var it = self.seek(start);
        it.end_key = end_key;
        return it;
    }

    fn findLeaf(self: BPlusTree, key: []const u8) []const u8 {
        var buf = self.fm.readPage(self.root);
        while (true) {
            const header = page.readHeader(buf);
            if (header.page_type == .leaf) return buf;
            const child_id = page.branchFindChild(buf, key, self.cmp);
            buf = self.fm.readPage(child_id);
        }
    }

    fn findLeftmostLeaf(self: BPlusTree) []const u8 {
        var buf = self.fm.readPage(self.root);
        while (true) {
            const header = page.readHeader(buf);
            if (header.page_type == .leaf) return buf;
            const count = page.branchEntryCount(buf);
            if (count == 0) {
                buf = self.fm.readPage(page.branchGetRightChild(buf));
            } else {
                buf = self.fm.readPage(page.branchGetChild(buf, 0));
            }
        }
    }

    fn findRightmostLeaf(self: BPlusTree) []const u8 {
        var buf = self.fm.readPage(self.root);
        while (true) {
            const header = page.readHeader(buf);
            if (header.page_type == .leaf) return buf;
            buf = self.fm.readPage(page.branchGetRightChild(buf));
        }
    }

    fn emptyIterator(self: BPlusTree) Iterator {
        return .{
            .tree = self,
            .leaf_buf = &[_]u8{},
            .slot = 0,
            .entry_count = 0,
            .end_key = null,
            .exhausted = true,
        };
    }
};

pub const Iterator = struct {
    tree: BPlusTree,
    leaf_buf: []const u8,
    slot: u16,
    entry_count: u16,
    end_key: ?[]const u8,
    exhausted: bool,

    pub fn next(self: *Iterator) ?Entry {
        if (self.exhausted) return null;
        // Cross leaf boundary if past end of current leaf
        if (self.slot >= self.entry_count) {
            const next_id = page.getLeafNextPage(self.leaf_buf);
            if (next_id == 0) {
                self.exhausted = true;
                return null;
            }
            self.leaf_buf = self.tree.fm.readPage(next_id);
            self.entry_count = page.leafEntryCount(self.leaf_buf);
            self.slot = 0;
        }
        const key = page.leafGetKey(self.leaf_buf, self.slot);
        // Check upper bound for range scans
        if (self.end_key) |ek| {
            if (self.tree.cmp(key, ek) != .lt) {
                self.exhausted = true;
                return null;
            }
        }
        const value = page.leafGetValue(self.leaf_buf, self.slot);
        self.slot += 1;
        return .{ .key = key, .value = value };
    }

    /// prev() does NOT check the exhausted flag — allows reverse after forward exhaustion.
    pub fn prev(self: *Iterator) ?Entry {
        if (self.slot > 0) {
            self.slot -= 1;
            self.exhausted = false;
            return .{
                .key = page.leafGetKey(self.leaf_buf, self.slot),
                .value = page.leafGetValue(self.leaf_buf, self.slot),
            };
        }
        // slot == 0, follow prev page
        if (self.leaf_buf.len == 0) return null;
        const prev_id = page.getLeafPrevPage(self.leaf_buf);
        if (prev_id == 0) return null;
        self.leaf_buf = self.tree.fm.readPage(prev_id);
        self.entry_count = page.leafEntryCount(self.leaf_buf);
        self.slot = self.entry_count - 1;
        self.exhausted = false;
        return .{
            .key = page.leafGetKey(self.leaf_buf, self.slot),
            .value = page.leafGetValue(self.leaf_buf, self.slot),
        };
    }

    pub fn peek(self: *Iterator) ?Entry {
        const saved_leaf = self.leaf_buf;
        const saved_slot = self.slot;
        const saved_count = self.entry_count;
        const saved_exhausted = self.exhausted;
        const result = self.next();
        self.leaf_buf = saved_leaf;
        self.slot = saved_slot;
        self.entry_count = saved_count;
        self.exhausted = saved_exhausted;
        return result;
    }
};

// ============================================================================
// Tests
// ============================================================================

fn testKeyCmp(a: []const u8, b: []const u8) math.Order {
    return mem.order(u8, a, b);
}

// --- Test tree builders ---

/// Tree A: Single leaf with 5 entries.
fn buildSingleLeafTree(fm: *file.FileManager) !u64 {
    const ps: usize = fm.page_size;
    const leaf_id = try fm.allocPage();
    var write_buf: [65536]u8 = undefined;
    const buf = write_buf[0..ps];
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "aaa", "v1");
    try page.leafInsertEntry(buf, 1, "bbb", "v2");
    try page.leafInsertEntry(buf, 2, "ccc", "v3");
    try page.leafInsertEntry(buf, 3, "ddd", "v4");
    try page.leafInsertEntry(buf, 4, "eee", "v5");
    try fm.writePage(leaf_id, buf);
    try fm.remap();
    return leaf_id;
}

/// Tree B: 2-level tree with 3 leaves (8 entries total).
fn buildTwoLevelTree(fm: *file.FileManager) !u64 {
    const ps: usize = fm.page_size;
    const leaf2 = try fm.allocPage();
    const leaf3 = try fm.allocPage();
    const leaf4 = try fm.allocPage();
    const branch5 = try fm.allocPage();

    var write_buf: [65536]u8 = undefined;
    const buf = write_buf[0..ps];

    // Leaf 2: "aaa","bbb","ccc" (next=3)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "aaa", "aaa");
    try page.leafInsertEntry(buf, 1, "bbb", "bbb");
    try page.leafInsertEntry(buf, 2, "ccc", "ccc");
    page.setLeafNextPage(buf, leaf3);
    try fm.writePage(leaf2, buf);

    // Leaf 3: "ddd","eee","fff" (prev=2, next=4)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "ddd", "ddd");
    try page.leafInsertEntry(buf, 1, "eee", "eee");
    try page.leafInsertEntry(buf, 2, "fff", "fff");
    page.setLeafPrevPage(buf, leaf2);
    page.setLeafNextPage(buf, leaf4);
    try fm.writePage(leaf3, buf);

    // Leaf 4: "ggg","hhh" (prev=3)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "ggg", "ggg");
    try page.leafInsertEntry(buf, 1, "hhh", "hhh");
    page.setLeafPrevPage(buf, leaf3);
    try fm.writePage(leaf4, buf);

    // Branch 5: sep "ddd"→child 2, sep "ggg"→child 3, right_child=4
    page.initBranch(buf, .eav, leaf4);
    try page.branchInsertEntry(buf, 0, "ddd", leaf2);
    try page.branchInsertEntry(buf, 1, "ggg", leaf3);
    try fm.writePage(branch5, buf);

    try fm.remap();
    return branch5;
}

/// Tree C: 3-level tree with 5 leaves (11 entries total).
fn buildThreeLevelTree(fm: *file.FileManager) !u64 {
    const ps: usize = fm.page_size;
    const leaf2 = try fm.allocPage();
    const leaf3 = try fm.allocPage();
    const leaf4 = try fm.allocPage();
    const leaf5 = try fm.allocPage();
    const leaf6 = try fm.allocPage();
    const branch7 = try fm.allocPage();
    const branch8 = try fm.allocPage();
    const root9 = try fm.allocPage();

    var write_buf: [65536]u8 = undefined;
    const buf = write_buf[0..ps];

    // Leaf 2: "aaa","bbb" (next=3)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "aaa", "aaa");
    try page.leafInsertEntry(buf, 1, "bbb", "bbb");
    page.setLeafNextPage(buf, leaf3);
    try fm.writePage(leaf2, buf);

    // Leaf 3: "ccc","ddd" (prev=2, next=4)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "ccc", "ccc");
    try page.leafInsertEntry(buf, 1, "ddd", "ddd");
    page.setLeafPrevPage(buf, leaf2);
    page.setLeafNextPage(buf, leaf4);
    try fm.writePage(leaf3, buf);

    // Leaf 4: "eee","fff" (prev=3, next=5)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "eee", "eee");
    try page.leafInsertEntry(buf, 1, "fff", "fff");
    page.setLeafPrevPage(buf, leaf3);
    page.setLeafNextPage(buf, leaf5);
    try fm.writePage(leaf4, buf);

    // Leaf 5: "ggg","hhh","iii" (prev=4, next=6)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "ggg", "ggg");
    try page.leafInsertEntry(buf, 1, "hhh", "hhh");
    try page.leafInsertEntry(buf, 2, "iii", "iii");
    page.setLeafPrevPage(buf, leaf4);
    page.setLeafNextPage(buf, leaf6);
    try fm.writePage(leaf5, buf);

    // Leaf 6: "jjj","kkk" (prev=5)
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "jjj", "jjj");
    try page.leafInsertEntry(buf, 1, "kkk", "kkk");
    page.setLeafPrevPage(buf, leaf5);
    try fm.writePage(leaf6, buf);

    // Branch 7: sep "ccc"→child 2, sep "eee"→child 3, right_child=4
    page.initBranch(buf, .eav, leaf4);
    try page.branchInsertEntry(buf, 0, "ccc", leaf2);
    try page.branchInsertEntry(buf, 1, "eee", leaf3);
    try fm.writePage(branch7, buf);

    // Branch 8: sep "jjj"→child 5, right_child=6
    page.initBranch(buf, .eav, leaf6);
    try page.branchInsertEntry(buf, 0, "jjj", leaf5);
    try fm.writePage(branch8, buf);

    // Root Branch 9: sep "ggg"→child 7, right_child=8
    page.initBranch(buf, .eav, branch8);
    try page.branchInsertEntry(buf, 0, "ggg", branch7);
    try fm.writePage(root9, buf);

    try fm.remap();
    return root9;
}

// --- Phase A: Empty Tree ---

test "subtask 1: lookup on empty tree returns null" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const tree = BPlusTree.init(0, &fm, testKeyCmp);
    try testing.expect(tree.lookup("anything") == null);
    try testing.expect(tree.lookup("") == null);
}

test "subtask 2: empty tree iterators return null" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const tree = BPlusTree.init(0, &fm, testKeyCmp);

    var it1 = tree.seek("key");
    try testing.expect(it1.next() == null);
    try testing.expect(it1.prev() == null);

    var it2 = tree.seekFirst();
    try testing.expect(it2.next() == null);
    try testing.expect(it2.prev() == null);

    var it3 = tree.seekLast();
    try testing.expect(it3.next() == null);
    try testing.expect(it3.prev() == null);

    var it4 = tree.range("a", "z");
    try testing.expect(it4.next() == null);
    try testing.expect(it4.prev() == null);
}

// --- Phase B: Single-Leaf Lookup ---

test "subtask 3: single leaf lookup existing keys" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildSingleLeafTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    try testing.expectEqualStrings("v1", tree.lookup("aaa").?);
    try testing.expectEqualStrings("v2", tree.lookup("bbb").?);
    try testing.expectEqualStrings("v3", tree.lookup("ccc").?);
    try testing.expectEqualStrings("v4", tree.lookup("ddd").?);
    try testing.expectEqualStrings("v5", tree.lookup("eee").?);

    // Missing keys
    try testing.expect(tree.lookup("zzz") == null);
    try testing.expect(tree.lookup("000") == null);
}

test "subtask 4: single leaf lookup between entries returns null" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildSingleLeafTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    try testing.expect(tree.lookup("aab") == null);
    try testing.expect(tree.lookup("bba") == null);
    try testing.expect(tree.lookup("ccz") == null);
    try testing.expect(tree.lookup("dda") == null);
}

// --- Phase C: Single-Leaf Iteration ---

test "subtask 5: single leaf seekFirst forward" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildSingleLeafTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    const expected = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee" };
    var it = tree.seekFirst();
    for (expected) |key| {
        const entry = it.next().?;
        try testing.expectEqualStrings(key, entry.key);
    }
    try testing.expect(it.next() == null);
}

test "subtask 6: single leaf seekLast backward" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildSingleLeafTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    const expected = [_][]const u8{ "eee", "ddd", "ccc", "bbb", "aaa" };
    var it = tree.seekLast();
    for (expected) |key| {
        const entry = it.prev().?;
        try testing.expectEqualStrings(key, entry.key);
    }
    try testing.expect(it.prev() == null);
}

test "subtask 7: single leaf bidirectional from seek" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildSingleLeafTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // Forward from seek("ccc")
    var it = tree.seek("ccc");
    try testing.expectEqualStrings("ccc", it.next().?.key);
    try testing.expectEqualStrings("ddd", it.next().?.key);
    try testing.expectEqualStrings("eee", it.next().?.key);
    try testing.expect(it.next() == null);

    // Backward from seek("ccc")
    var it2 = tree.seek("ccc");
    try testing.expectEqualStrings("bbb", it2.prev().?.key);
}

// --- Phase D: Multi-Leaf Iteration ---

test "subtask 8: multi-leaf forward iteration" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildTwoLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    const expected = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh" };
    var it = tree.seekFirst();
    for (expected) |key| {
        const entry = it.next().?;
        try testing.expectEqualStrings(key, entry.key);
    }
    try testing.expect(it.next() == null);
}

test "subtask 9: multi-leaf backward iteration" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildTwoLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    const expected = [_][]const u8{ "hhh", "ggg", "fff", "eee", "ddd", "ccc", "bbb", "aaa" };
    var it = tree.seekLast();
    for (expected) |key| {
        const entry = it.prev().?;
        try testing.expectEqualStrings(key, entry.key);
    }
    try testing.expect(it.prev() == null);
}

test "subtask 10: multi-leaf seek accuracy" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildTwoLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // Exact match at start of first leaf
    var it1 = tree.seek("aaa");
    try testing.expectEqualStrings("aaa", it1.next().?.key);

    // Between entries in first leaf
    var it2 = tree.seek("bbc");
    try testing.expectEqualStrings("ccc", it2.next().?.key);

    // Exact match at leaf boundary
    var it3 = tree.seek("ddd");
    try testing.expectEqualStrings("ddd", it3.next().?.key);

    // Between entries in middle leaf
    var it4 = tree.seek("eef");
    try testing.expectEqualStrings("fff", it4.next().?.key);

    // Exact match at last leaf
    var it5 = tree.seek("ggg");
    try testing.expectEqualStrings("ggg", it5.next().?.key);
}

test "subtask 11: multi-leaf backward crossing from seek" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildTwoLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // seek("ddd") positions at leaf 3 slot 0; prev() crosses to leaf 2
    var it = tree.seek("ddd");
    try testing.expectEqualStrings("ccc", it.prev().?.key);
}

// --- Phase E: Range Scans ---

test "subtask 12: range scan single leaf" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildSingleLeafTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // range("bbb","ddd") → "bbb","ccc" (ddd excluded)
    var it = tree.range("bbb", "ddd");
    try testing.expectEqualStrings("bbb", it.next().?.key);
    try testing.expectEqualStrings("ccc", it.next().?.key);
    try testing.expect(it.next() == null);
}

test "subtask 13: range scan crossing leaf boundaries" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildTwoLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // range("bbb","fff") → "bbb","ccc","ddd","eee" (fff excluded)
    var it = tree.range("bbb", "fff");
    try testing.expectEqualStrings("bbb", it.next().?.key);
    try testing.expectEqualStrings("ccc", it.next().?.key);
    try testing.expectEqualStrings("ddd", it.next().?.key);
    try testing.expectEqualStrings("eee", it.next().?.key);
    try testing.expect(it.next() == null);
}

test "subtask 14: empty range scans" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildTwoLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // range past all entries → empty
    var it1 = tree.range("iii", "jjj");
    try testing.expect(it1.next() == null);

    // range where start == end → empty (half-open)
    var it2 = tree.range("aaa", "aaa");
    try testing.expect(it2.next() == null);
}

// --- Phase F: Deep Tree ---

test "subtask 15: three-level tree lookup" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildThreeLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // All 11 keys present
    const keys = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj", "kkk" };
    for (keys) |key| {
        const val = tree.lookup(key);
        try testing.expect(val != null);
        try testing.expectEqualStrings(key, val.?);
    }

    // Missing keys
    try testing.expect(tree.lookup("000") == null);
    try testing.expect(tree.lookup("bbc") == null);
    try testing.expect(tree.lookup("zzz") == null);
}

test "subtask 16: three-level tree full traversal" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildThreeLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    const keys = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj", "kkk" };

    // Forward traversal
    var fwd = tree.seekFirst();
    for (keys) |key| {
        try testing.expectEqualStrings(key, fwd.next().?.key);
    }
    try testing.expect(fwd.next() == null);

    // Backward traversal
    var bwd = tree.seekLast();
    var i: usize = keys.len;
    while (i > 0) {
        i -= 1;
        try testing.expectEqualStrings(keys[i], bwd.prev().?.key);
    }
    try testing.expect(bwd.prev() == null);
}

// --- Phase G: Edge Case ---

test "subtask 17: reverse from exhausted seek" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const root = try buildTwoLevelTree(&fm);
    const tree = BPlusTree.init(root, &fm, testKeyCmp);

    // seek("zzz") → exhausted (past all keys)
    var it = tree.seek("zzz");
    try testing.expect(it.next() == null);

    // prev() from exhausted position → "hhh" (last entry)
    try testing.expectEqualStrings("hhh", it.prev().?.key);
}
