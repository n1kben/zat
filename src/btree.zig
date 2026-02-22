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
const freelist = @import("freelist.zig");

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub const BPlusTree = struct {
    root: u64,
    fm: *file.FileManager,
    cmp: page.KeyCompareFn,
    freed: ?*freelist.FreePageTracker = null,
    index_id: page.IndexId = .eav,

    pub fn init(root_page: u64, fm: *file.FileManager, cmp: page.KeyCompareFn) BPlusTree {
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

    // ====================================================================
    // Write operations (COW insert/delete)
    // ====================================================================

    const MAX_DEPTH: u8 = 16;

    const PathEntry = struct {
        page_id: u64,
        child_idx: u16,
        went_right: bool, // true if we followed the right_child pointer
    };

    const PropagateState = union(enum) {
        cow: u64, // new page id replacing old child
        split: struct {
            sep_key: []const u8,
            left_id: u64,
            right_id: u64,
        },
    };

    pub fn insert(self: *BPlusTree, key: []const u8, value: []const u8) !void {
        const ps: usize = self.fm.page_size;

        if (self.root == 0) {
            // Empty tree: create a single leaf
            const leaf_id = try self.fm.allocPage();
            var buf: [65536]u8 = undefined;
            const b = buf[0..ps];
            page.initLeaf(b, self.index_id);
            try page.leafInsertEntry(b, 0, key, value);
            try self.fm.writePage(leaf_id, b);
            try self.fm.remap();
            self.root = leaf_id;
            return;
        }

        // Descend with path tracking
        var path: [MAX_DEPTH]PathEntry = undefined;
        var depth: u8 = 0;
        var cur_page_id = self.root;

        while (true) {
            const cur_buf = self.fm.readPage(cur_page_id);
            const header = page.readHeader(cur_buf);
            if (header.page_type == .leaf) break;

            // Branch: find which child to follow
            const count = page.branchEntryCount(cur_buf);
            var child_idx: u16 = 0;
            var went_right = true;
            var child_id: u64 = page.branchGetRightChild(cur_buf);

            for (0..count) |i| {
                const sep_key = page.branchGetKey(cur_buf, @intCast(i));
                switch (self.cmp(key, sep_key)) {
                    .lt => {
                        child_id = page.branchGetChild(cur_buf, @intCast(i));
                        child_idx = @intCast(i);
                        went_right = false;
                        break;
                    },
                    .eq, .gt => {},
                }
            }

            path[depth] = .{ .page_id = cur_page_id, .child_idx = child_idx, .went_right = went_right };
            depth += 1;
            cur_page_id = child_id;
        }

        // cur_page_id is the leaf
        const old_leaf_id = cur_page_id;
        var leaf_buf: [65536]u8 = undefined;
        const lb = leaf_buf[0..ps];
        const old_leaf = self.fm.readPage(old_leaf_id);
        @memcpy(lb, old_leaf);

        // If key exists, delete old entry first (update semantics)
        if (page.leafFindKey(lb, key, self.cmp)) |existing_idx| {
            page.leafDeleteEntry(lb, existing_idx);
        }

        // Find insertion point
        const insert_idx = page.leafSearchPoint(lb, key, self.cmp);

        // Try inserting
        var state: PropagateState = undefined;

        if (page.leafInsertEntry(lb, insert_idx, key, value)) |_| {
            // Success — COW the leaf
            const new_leaf_id = try self.fm.allocPage();
            try self.fm.writePage(new_leaf_id, lb);
            try self.fixLeafSiblings(old_leaf_id, lb, new_leaf_id, null);
            if (self.freed) |f| f.addPage(old_leaf_id);
            state = .{ .cow = new_leaf_id };
        } else |_| {
            // PageFull — split
            var right_buf: [65536]u8 = undefined;
            const rb = right_buf[0..ps];
            const split_result = page.leafSplit(lb, rb);

            // Determine which half gets the new entry
            const sep = split_result.separator_key;
            // We need to copy sep before modifying pages
            var sep_copy: [4096]u8 = undefined;
            @memcpy(sep_copy[0..sep.len], sep);
            const sep_saved = sep_copy[0..sep.len];

            switch (self.cmp(key, sep_saved)) {
                .lt => {
                    const idx = page.leafSearchPoint(lb, key, self.cmp);
                    try page.leafInsertEntry(lb, idx, key, value);
                },
                .eq, .gt => {
                    const idx = page.leafSearchPoint(rb, key, self.cmp);
                    try page.leafInsertEntry(rb, idx, key, value);
                },
            }

            const new_left_id = try self.fm.allocPage();
            const new_right_id = try self.fm.allocPage();

            // Set sibling pointers within the split pair
            page.setLeafNextPage(lb, new_right_id);
            page.setLeafPrevPage(rb, new_left_id);

            try self.fm.writePage(new_left_id, lb);
            try self.fm.writePage(new_right_id, rb);

            // Fix neighbors
            try self.fixLeafSiblings(old_leaf_id, old_leaf, new_left_id, new_right_id);
            if (self.freed) |f| f.addPage(old_leaf_id);

            state = .{ .split = .{ .sep_key = sep_saved, .left_id = new_left_id, .right_id = new_right_id } };
        }

        // Propagate upward
        var level: u8 = depth;
        while (level > 0) {
            level -= 1;
            const pe = path[level];
            const branch_buf_mmap = self.fm.readPage(pe.page_id);
            var branch_buf: [65536]u8 = undefined;
            const bb = branch_buf[0..ps];
            @memcpy(bb, branch_buf_mmap);

            switch (state) {
                .cow => |new_child_id| {
                    if (pe.went_right) {
                        page.branchSetRightChild(bb, new_child_id);
                    } else {
                        page.branchSetChild(bb, pe.child_idx, new_child_id);
                    }
                    const new_branch_id = try self.fm.allocPage();
                    try self.fm.writePage(new_branch_id, bb);
                    if (self.freed) |f| f.addPage(pe.page_id);
                    state = .{ .cow = new_branch_id };
                },
                .split => |s| {
                    // Try inserting the separator into the branch
                    var insert_at: u16 = undefined;
                    if (pe.went_right) {
                        // We followed right_child, so new separator goes at end
                        insert_at = page.branchEntryCount(bb);
                    } else {
                        insert_at = pe.child_idx;
                    }

                    if (page.branchInsertEntry(bb, insert_at, s.sep_key, s.left_id)) |_| {
                                    // Success — update the child pointer after the separator
                        if (pe.went_right) {
                            page.branchSetRightChild(bb, s.right_id);
                        } else {
                            // The entry at insert_at now has left_id as child.
                            // The entry at insert_at+1 (or right_child) should point to right_id.
                            if (insert_at + 1 < page.branchEntryCount(bb)) {
                                page.branchSetChild(bb, insert_at + 1, s.right_id);
                            } else {
                                page.branchSetRightChild(bb, s.right_id);
                            }
                        }
                        const new_branch_id = try self.fm.allocPage();
                        try self.fm.writePage(new_branch_id, bb);
                        if (self.freed) |f| f.addPage(pe.page_id);
                        state = .{ .cow = new_branch_id };
                    } else |_| {
                        // Branch is full — split it
                        var right_branch_buf: [65536]u8 = undefined;
                        const rbb = right_branch_buf[0..ps];

                        const bsplit = page.branchSplit(bb, rbb);
                        var promoted_key_copy: [4096]u8 = undefined;
                        @memcpy(promoted_key_copy[0..bsplit.separator_key.len], bsplit.separator_key);
                        const promoted_key = promoted_key_copy[0..bsplit.separator_key.len];

                        // Now insert the child-split separator into the correct half
                        switch (self.cmp(s.sep_key, promoted_key)) {
                            .lt => {
                                // Goes into left (bb)
                                const idx = branchSearchPoint(bb, s.sep_key, self.cmp);
                                page.branchInsertEntry(bb, idx, s.sep_key, s.left_id) catch unreachable;
                                if (idx + 1 < page.branchEntryCount(bb)) {
                                    page.branchSetChild(bb, idx + 1, s.right_id);
                                } else {
                                    page.branchSetRightChild(bb, s.right_id);
                                }
                            },
                            .eq, .gt => {
                                // Goes into right (rbb)
                                const idx = branchSearchPoint(rbb, s.sep_key, self.cmp);
                                page.branchInsertEntry(rbb, idx, s.sep_key, s.left_id) catch unreachable;
                                if (idx + 1 < page.branchEntryCount(rbb)) {
                                    page.branchSetChild(rbb, idx + 1, s.right_id);
                                } else {
                                    page.branchSetRightChild(rbb, s.right_id);
                                }
                            },
                        }

                        const new_left_branch = try self.fm.allocPage();
                        const new_right_branch = try self.fm.allocPage();
                        try self.fm.writePage(new_left_branch, bb);
                        try self.fm.writePage(new_right_branch, rbb);
                        if (self.freed) |f| f.addPage(pe.page_id);
                        state = .{ .split = .{ .sep_key = promoted_key, .left_id = new_left_branch, .right_id = new_right_branch } };
                    }
                },
            }
        }

        // Set new root
        switch (state) {
            .cow => |new_root_id| {
                self.root = new_root_id;
            },
            .split => |s| {
                // Create new root branch
                var root_buf: [65536]u8 = undefined;
                const rootb = root_buf[0..ps];
                page.initBranch(rootb, self.index_id, s.right_id);
                try page.branchInsertEntry(rootb, 0, s.sep_key, s.left_id);
                const new_root_id = try self.fm.allocPage();
                try self.fm.writePage(new_root_id, rootb);
                self.root = new_root_id;
            },
        }

        try self.fm.remap();
    }

    pub fn delete(self: *BPlusTree, key: []const u8) !void {
        if (self.root == 0) return;

        const ps: usize = self.fm.page_size;

        // Descend with path tracking
        var path: [MAX_DEPTH]PathEntry = undefined;
        var depth: u8 = 0;
        var cur_page_id = self.root;

        while (true) {
            const cur_buf = self.fm.readPage(cur_page_id);
            const header = page.readHeader(cur_buf);
            if (header.page_type == .leaf) break;

            const count = page.branchEntryCount(cur_buf);
            var child_idx: u16 = 0;
            var went_right = true;
            var child_id: u64 = page.branchGetRightChild(cur_buf);

            for (0..count) |i| {
                const sep_key = page.branchGetKey(cur_buf, @intCast(i));
                switch (self.cmp(key, sep_key)) {
                    .lt => {
                        child_id = page.branchGetChild(cur_buf, @intCast(i));
                        child_idx = @intCast(i);
                        went_right = false;
                        break;
                    },
                    .eq, .gt => {},
                }
            }

            path[depth] = .{ .page_id = cur_page_id, .child_idx = child_idx, .went_right = went_right };
            depth += 1;
            cur_page_id = child_id;
        }

        // At the leaf
        const old_leaf_id = cur_page_id;
        const old_leaf = self.fm.readPage(old_leaf_id);

        // Check if key exists
        const slot = page.leafFindKey(old_leaf, key, self.cmp) orelse return;

        // COW copy + delete
        var leaf_buf: [65536]u8 = undefined;
        const lb = leaf_buf[0..ps];
        @memcpy(lb, old_leaf);
        page.leafDeleteEntry(lb, slot);

        const new_leaf_id = try self.fm.allocPage();
        try self.fm.writePage(new_leaf_id, lb);
        try self.fixLeafSiblings(old_leaf_id, old_leaf, new_leaf_id, null);
        if (self.freed) |f| f.addPage(old_leaf_id);

        // Propagate COW upward
        var new_child_id = new_leaf_id;
        var level: u8 = depth;
        while (level > 0) {
            level -= 1;
            const pe = path[level];
            const branch_buf_mmap = self.fm.readPage(pe.page_id);
            var branch_buf: [65536]u8 = undefined;
            const bb = branch_buf[0..ps];
            @memcpy(bb, branch_buf_mmap);

            if (pe.went_right) {
                page.branchSetRightChild(bb, new_child_id);
            } else {
                page.branchSetChild(bb, pe.child_idx, new_child_id);
            }
            const new_branch_id = try self.fm.allocPage();
            try self.fm.writePage(new_branch_id, bb);
            if (self.freed) |f| f.addPage(pe.page_id);
            new_child_id = new_branch_id;
        }

        self.root = new_child_id;
        try self.fm.remap();
    }

    /// Fix sibling pointers for neighbors of a COW'd leaf.
    /// For a simple COW (no split), new_right_id is null.
    /// For a split, new_right_id is the right half's page id.
    fn fixLeafSiblings(self: *BPlusTree, old_leaf_id: u64, old_leaf_data: []const u8, new_left_id: u64, new_right_id: ?u64) !void {
        const ps: usize = self.fm.page_size;
        const prev_id = page.getLeafPrevPage(old_leaf_data);
        const next_id = page.getLeafNextPage(old_leaf_data);

        if (new_right_id) |right_id| {
            // Split case: old_prev.next → new_left_id, old_next.prev → right_id
            if (prev_id != 0) {
                var buf: [65536]u8 = undefined;
                const b = buf[0..ps];
                @memcpy(b, self.fm.readPage(prev_id));
                page.setLeafNextPage(b, new_left_id);
                try self.fm.writePage(prev_id, b);
            }
            if (next_id != 0) {
                var buf: [65536]u8 = undefined;
                const b = buf[0..ps];
                @memcpy(b, self.fm.readPage(next_id));
                page.setLeafPrevPage(b, right_id);
                try self.fm.writePage(next_id, b);
            }
        } else {
            // Simple COW: just update neighbors to point to new_left_id
            if (prev_id != 0) {
                var buf: [65536]u8 = undefined;
                const b = buf[0..ps];
                @memcpy(b, self.fm.readPage(prev_id));
                page.setLeafNextPage(b, new_left_id);
                try self.fm.writePage(prev_id, b);
            }
            if (next_id != 0) {
                var buf: [65536]u8 = undefined;
                const b = buf[0..ps];
                @memcpy(b, self.fm.readPage(next_id));
                page.setLeafPrevPage(b, new_left_id);
                try self.fm.writePage(next_id, b);
            }
        }
        _ = old_leaf_id;
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

/// Lower-bound search in a branch page — returns the index where a key should be inserted.
fn branchSearchPoint(buf: []const u8, key: []const u8, cmp: page.KeyCompareFn) u16 {
    const count = page.branchEntryCount(buf);
    var lo: u16 = 0;
    var hi: u16 = count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const entry_key = page.branchGetKey(buf, mid);
        switch (cmp(entry_key, key)) {
            .lt => lo = mid + 1,
            .gt, .eq => hi = mid,
        }
    }
    return lo;
}

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

// --- Phase H: Insert Basics ---

test "insert 1: insert into empty tree" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("hello", "world");

    try testing.expect(tree.root != 0);
    try testing.expectEqualStrings("world", tree.lookup("hello").?);
}

test "insert 2: insert 5 entries no split" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("ccc", "v3");
    try tree.insert("aaa", "v1");
    try tree.insert("eee", "v5");
    try tree.insert("bbb", "v2");
    try tree.insert("ddd", "v4");

    // All lookable
    try testing.expectEqualStrings("v1", tree.lookup("aaa").?);
    try testing.expectEqualStrings("v2", tree.lookup("bbb").?);
    try testing.expectEqualStrings("v3", tree.lookup("ccc").?);
    try testing.expectEqualStrings("v4", tree.lookup("ddd").?);
    try testing.expectEqualStrings("v5", tree.lookup("eee").?);

    // Forward iteration correct order
    const expected = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee" };
    var it = tree.seekFirst();
    for (expected) |key| {
        try testing.expectEqualStrings(key, it.next().?.key);
    }
    try testing.expect(it.next() == null);
}

test "insert 3: insert duplicate key (update)" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("aaa", "old");
    try tree.insert("bbb", "v2");
    try tree.insert("aaa", "new");

    try testing.expectEqualStrings("new", tree.lookup("aaa").?);
    try testing.expectEqualStrings("v2", tree.lookup("bbb").?);

    // Entry count should be 2, not 3
    const leaf = fm.readPage(tree.root);
    try testing.expectEqual(@as(u16, 2), page.leafEntryCount(leaf));
}

test "insert 4: insert then lookup missing key" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("aaa", "v1");
    try tree.insert("ccc", "v3");

    try testing.expect(tree.lookup("bbb") == null);
    try testing.expect(tree.lookup("ddd") == null);
    try testing.expect(tree.lookup("000") == null);
}

// --- Phase I: Insert with Leaf Split ---

test "insert 5: leaf split triggers (small page)" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 256 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);

    // Insert entries until a split must occur. With 256-byte pages:
    // leaf header=24, each entry ≈ 2+3+2+2 = 9 data + 2 offset = 11 bytes
    // Available: 232 / 11 ≈ 21 entries max. Use enough to guarantee split.
    const keys = [_][]const u8{ "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn", "oo", "pp", "qq", "rr", "ss", "tt", "uu", "vv", "ww" };
    for (keys) |k| {
        try tree.insert(k, k);
    }

    // All entries lookable
    for (keys) |k| {
        const val = tree.lookup(k);
        try testing.expect(val != null);
        try testing.expectEqualStrings(k, val.?);
    }
}

test "insert 6: after split, seekFirst/seekLast traversal works" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 256 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    const keys = [_][]const u8{ "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn", "oo", "pp", "qq", "rr", "ss", "tt" };
    for (keys) |k| {
        try tree.insert(k, k);
    }

    // Forward traversal
    var fwd = tree.seekFirst();
    for (keys) |k| {
        const entry = fwd.next() orelse {
            return error.TestUnexpectedResult;
        };
        try testing.expectEqualStrings(k, entry.key);
    }
    try testing.expect(fwd.next() == null);

    // Backward traversal
    var bwd = tree.seekLast();
    var i: usize = keys.len;
    while (i > 0) {
        i -= 1;
        const entry = bwd.prev() orelse {
            return error.TestUnexpectedResult;
        };
        try testing.expectEqualStrings(keys[i], entry.key);
    }
    try testing.expect(bwd.prev() == null);
}

test "insert 7: multiple successive splits" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 128 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);

    // With 128-byte pages, splits happen very quickly
    // Header=24, each "xx"+"xx" entry = 2+2+2+2=8 data + 2 offset = 10
    // (128-24)/10 ≈ 10 max entries → should produce 3+ leaves
    const keys = [_][]const u8{ "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn", "oo", "pp", "qq", "rr", "ss", "tt" };
    for (keys) |k| {
        try tree.insert(k, k);
    }

    // All entries in order via forward iteration
    var it = tree.seekFirst();
    for (keys) |k| {
        const entry = it.next() orelse return error.TestUnexpectedResult;
        try testing.expectEqualStrings(k, entry.key);
    }
    try testing.expect(it.next() == null);
}

// --- Phase J: Insert with Branch Split ---

test "insert 8: branch split" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 128 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);

    // Insert enough entries to trigger branch splits (128-byte pages)
    var key_buf: [2]u8 = undefined;
    var i: u8 = 0;
    while (i < 60) : (i += 1) {
        key_buf[0] = 'a' + (i / 26);
        key_buf[1] = 'a' + (i % 26);
        try tree.insert(&key_buf, &key_buf);
    }

    // All entries lookable
    i = 0;
    while (i < 60) : (i += 1) {
        key_buf[0] = 'a' + (i / 26);
        key_buf[1] = 'a' + (i % 26);
        const val = tree.lookup(&key_buf);
        try testing.expect(val != null);
        try testing.expectEqualStrings(&key_buf, val.?);
    }
}

test "insert 9: continued inserts after branch split" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 128 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);

    // Insert 100 entries — should create multiple branch levels
    var key_buf: [3]u8 = undefined;
    var i: u16 = 0;
    while (i < 100) : (i += 1) {
        key_buf[0] = 'a' + @as(u8, @intCast(i / 26 / 26));
        key_buf[1] = 'a' + @as(u8, @intCast((i / 26) % 26));
        key_buf[2] = 'a' + @as(u8, @intCast(i % 26));
        try tree.insert(&key_buf, &key_buf);
    }

    // All entries lookable
    i = 0;
    while (i < 100) : (i += 1) {
        key_buf[0] = 'a' + @as(u8, @intCast(i / 26 / 26));
        key_buf[1] = 'a' + @as(u8, @intCast((i / 26) % 26));
        key_buf[2] = 'a' + @as(u8, @intCast(i % 26));
        const val = tree.lookup(&key_buf);
        try testing.expect(val != null);
        try testing.expectEqualStrings(&key_buf, val.?);
    }

    // Forward iteration in order
    var it = tree.seekFirst();
    var prev_key: [3]u8 = [_]u8{ 0, 0, 0 };
    var count: u16 = 0;
    while (it.next()) |entry| {
        if (count > 0) {
            try testing.expectEqual(math.Order.lt, mem.order(u8, &prev_key, entry.key));
        }
        @memcpy(&prev_key, entry.key);
        count += 1;
    }
    try testing.expectEqual(@as(u16, 100), count);
}

// --- Phase K: Delete Basics ---

test "delete 10: delete from single leaf" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("aaa", "v1");
    try tree.insert("bbb", "v2");
    try tree.insert("ccc", "v3");

    const old_root = tree.root;
    try tree.delete("bbb");

    // Root changed (COW)
    try testing.expect(tree.root != old_root);
    // Key gone
    try testing.expect(tree.lookup("bbb") == null);
    // Others remain
    try testing.expectEqualStrings("v1", tree.lookup("aaa").?);
    try testing.expectEqualStrings("v3", tree.lookup("ccc").?);
}

test "delete 11: delete non-existent key" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("aaa", "v1");
    try tree.insert("bbb", "v2");

    const old_root = tree.root;
    try tree.delete("zzz");

    // Root unchanged — no COW needed
    try testing.expectEqual(old_root, tree.root);
    // All entries intact
    try testing.expectEqualStrings("v1", tree.lookup("aaa").?);
    try testing.expectEqualStrings("v2", tree.lookup("bbb").?);
}

test "delete 12: delete all entries one by one" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("aaa", "v1");
    try tree.insert("bbb", "v2");
    try tree.insert("ccc", "v3");

    try tree.delete("bbb");
    try testing.expect(tree.lookup("bbb") == null);
    try testing.expectEqualStrings("v1", tree.lookup("aaa").?);
    try testing.expectEqualStrings("v3", tree.lookup("ccc").?);

    try tree.delete("aaa");
    try testing.expect(tree.lookup("aaa") == null);
    try testing.expectEqualStrings("v3", tree.lookup("ccc").?);

    try tree.delete("ccc");
    try testing.expect(tree.lookup("ccc") == null);
}

// --- Phase L: Delete from Multi-Level Tree ---

test "delete 13: delete from 2-level tree" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 256 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    const keys = [_][]const u8{ "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn", "oo", "pp", "qq", "rr", "ss", "tt" };
    for (keys) |k| {
        try tree.insert(k, k);
    }

    // Delete a few
    try tree.delete("ee");
    try tree.delete("mm");
    try tree.delete("aa");

    try testing.expect(tree.lookup("ee") == null);
    try testing.expect(tree.lookup("mm") == null);
    try testing.expect(tree.lookup("aa") == null);

    // Remaining entries still in order via iteration
    var it = tree.seekFirst();
    var count: u16 = 0;
    var prev_key: [2]u8 = [_]u8{ 0, 0 };
    while (it.next()) |entry| {
        if (count > 0) {
            try testing.expectEqual(math.Order.lt, mem.order(u8, &prev_key, entry.key));
        }
        @memcpy(&prev_key, entry.key[0..2]);
        count += 1;
    }
    try testing.expectEqual(@as(u16, 17), count); // 20 - 3
}

test "delete 14: delete from 3-level tree, multiple keys" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 128 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);

    // Insert enough for a 3-level tree
    var key_buf: [2]u8 = undefined;
    var i: u8 = 0;
    while (i < 60) : (i += 1) {
        key_buf[0] = 'a' + (i / 26);
        key_buf[1] = 'a' + (i % 26);
        try tree.insert(&key_buf, &key_buf);
    }

    // Delete every 3rd entry
    i = 0;
    var deleted: u16 = 0;
    while (i < 60) : (i += 3) {
        key_buf[0] = 'a' + (i / 26);
        key_buf[1] = 'a' + (i % 26);
        try tree.delete(&key_buf);
        deleted += 1;
    }

    // Remaining entries correct and ordered
    var it = tree.seekFirst();
    var count: u16 = 0;
    var prev: [2]u8 = [_]u8{ 0, 0 };
    while (it.next()) |entry| {
        if (count > 0) {
            try testing.expectEqual(math.Order.lt, mem.order(u8, &prev, entry.key));
        }
        @memcpy(&prev, entry.key[0..2]);
        count += 1;
    }
    try testing.expectEqual(@as(u16, 60 - deleted), count);
}

// --- Phase M: Mixed Operations ---

test "insert/delete 15: insert, delete, re-insert same key" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);
    try tree.insert("key", "first");
    try testing.expectEqualStrings("first", tree.lookup("key").?);

    try tree.delete("key");
    try testing.expect(tree.lookup("key") == null);

    try tree.insert("key", "second");
    try testing.expectEqualStrings("second", tree.lookup("key").?);
}

test "insert/delete 16: stress insert 50+ then delete half" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 256 });
    defer fm.close();

    var tree = BPlusTree.init(0, &fm, testKeyCmp);

    // Insert 60 entries
    var key_buf: [3]u8 = undefined;
    var i: u16 = 0;
    while (i < 60) : (i += 1) {
        key_buf[0] = 'a' + @as(u8, @intCast(i / 26 / 26));
        key_buf[1] = 'a' + @as(u8, @intCast((i / 26) % 26));
        key_buf[2] = 'a' + @as(u8, @intCast(i % 26));
        try tree.insert(&key_buf, &key_buf);
    }

    // Delete even-indexed entries (30 entries)
    i = 0;
    while (i < 60) : (i += 2) {
        key_buf[0] = 'a' + @as(u8, @intCast(i / 26 / 26));
        key_buf[1] = 'a' + @as(u8, @intCast((i / 26) % 26));
        key_buf[2] = 'a' + @as(u8, @intCast(i % 26));
        try tree.delete(&key_buf);
    }

    // Remaining (odd-indexed) entries all present and ordered
    var it = tree.seekFirst();
    var count: u16 = 0;
    var prev: [3]u8 = [_]u8{ 0, 0, 0 };
    while (it.next()) |entry| {
        if (count > 0) {
            try testing.expectEqual(math.Order.lt, mem.order(u8, &prev, entry.key));
        }
        @memcpy(&prev, entry.key[0..3]);
        count += 1;
    }
    try testing.expectEqual(@as(u16, 30), count);

    // Verify deleted entries are gone
    i = 0;
    while (i < 60) : (i += 2) {
        key_buf[0] = 'a' + @as(u8, @intCast(i / 26 / 26));
        key_buf[1] = 'a' + @as(u8, @intCast((i / 26) % 26));
        key_buf[2] = 'a' + @as(u8, @intCast(i % 26));
        try testing.expect(tree.lookup(&key_buf) == null);
    }

    // Verify remaining entries are present
    i = 1;
    while (i < 60) : (i += 2) {
        key_buf[0] = 'a' + @as(u8, @intCast(i / 26 / 26));
        key_buf[1] = 'a' + @as(u8, @intCast((i / 26) % 26));
        key_buf[2] = 'a' + @as(u8, @intCast(i % 26));
        try testing.expect(tree.lookup(&key_buf) != null);
    }
}
