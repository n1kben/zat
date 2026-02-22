// ZatDB — Free page tracking and reclamation
//
// Tracks pages freed during COW B+ tree operations and manages the FreeDB
// (a B+ tree with index_id = .free) that persists freed page lists across
// transactions, enabling page reuse.
//
// FreeDB key/value format:
//   Key:   [tx_id: 8 bytes BE]
//   Value: [page_count: 4 bytes BE][page_id_1: 8 bytes BE][page_id_2: 8 bytes BE]...

const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;
const encoding = @import("encoding.zig");
const page = @import("page.zig");
const file = @import("file.zig");
const btree = @import("btree.zig");

// ============================================================================
// FreePageTracker — accumulates freed page IDs during COW operations
// ============================================================================

pub const MAX_TRACKED_PAGES: usize = 256;

pub const FreePageTracker = struct {
    pages: [MAX_TRACKED_PAGES]u64 = .{0} ** MAX_TRACKED_PAGES,
    count: u32 = 0,

    pub fn addPage(self: *FreePageTracker, page_id: u64) void {
        if (self.count < MAX_TRACKED_PAGES) {
            self.pages[self.count] = page_id;
            self.count += 1;
        }
    }

    pub fn reset(self: *FreePageTracker) void {
        self.count = 0;
    }

    pub fn getFreedPages(self: *const FreePageTracker) []const u64 {
        return self.pages[0..self.count];
    }
};

// ============================================================================
// FreeDB key/value encoding
// ============================================================================

/// Encode a tx_id as a FreeDB key (8 bytes big-endian).
pub fn encodeFreeKey(buf: *[8]u8, tx_id: u64) void {
    encoding.writeU64(buf, tx_id);
}

/// Decode a FreeDB key back to a tx_id.
pub fn decodeFreeKey(buf: *const [8]u8) u64 {
    return encoding.readU64(buf);
}

/// Encode freed page IDs as a FreeDB value.
/// Format: [page_count: 4 BE][page_id_1: 8 BE][page_id_2: 8 BE]...
/// Returns the number of bytes written.
pub fn encodeFreeValue(buf: []u8, pages: []const u64) usize {
    const count: u32 = @intCast(pages.len);
    encoding.writeU32(buf[0..4], count);
    var pos: usize = 4;
    for (pages) |pg| {
        encoding.writeU64(buf[pos..][0..8], pg);
        pos += 8;
    }
    return pos;
}

/// Decode a FreeDB value to get the page IDs.
/// Returns a slice of page IDs read into the provided output buffer.
pub fn decodeFreeValue(data: []const u8, out: []u64) []u64 {
    const count = encoding.readU32(data[0..4]);
    var pos: usize = 4;
    var i: u32 = 0;
    while (i < count and i < out.len) : (i += 1) {
        out[i] = encoding.readU64(data[pos..][0..8]);
        pos += 8;
    }
    return out[0..i];
}

/// Size of a FreeDB value for a given number of page IDs.
pub fn freeValueSize(page_count: usize) usize {
    return 4 + page_count * 8;
}

/// Key comparison for FreeDB: 8-byte big-endian u64 memcmp.
pub fn freeDbKeyCmp(a: []const u8, b: []const u8) math.Order {
    return mem.order(u8, a, b);
}

// ============================================================================
// FreePageManager — coordinates the FreeDB tree
// ============================================================================

pub const FreePageManager = struct {
    tree: btree.BPlusTree,
    tracker: FreePageTracker,
    carry_forward: FreePageTracker,
    oldest_active_reader: u64,

    pub fn init(free_root: u64, fm: *file.FileManager) FreePageManager {
        return .{
            .tree = btree.BPlusTree.init(free_root, fm, freeDbKeyCmp),
            .tracker = .{},
            .carry_forward = .{},
            .oldest_active_reader = math.maxInt(u64),
        };
    }

    /// Commit freed pages to the FreeDB.
    /// Merges carry_forward into the tracker, encodes and inserts into FreeDB,
    /// then captures FreeDB's own COW pages into a new carry_forward.
    pub fn commitFreedPages(self: *FreePageManager, tx_id: u64) !void {
        // Merge carry-forward pages into tracker
        const cf = self.carry_forward.getFreedPages();
        for (cf) |pg| {
            self.tracker.addPage(pg);
        }
        self.carry_forward.reset();

        const freed = self.tracker.getFreedPages();
        if (freed.len == 0) return;

        // Encode key
        var key_buf: [8]u8 = undefined;
        encodeFreeKey(&key_buf, tx_id);

        // Encode value
        var val_buf: [4 + MAX_TRACKED_PAGES * 8]u8 = undefined;
        const val_len = encodeFreeValue(&val_buf, freed);

        // Set up a fresh tracker for FreeDB's own COW pages
        self.tree.freed = &self.carry_forward;

        // Insert into FreeDB
        try self.tree.insert(&key_buf, val_buf[0..val_len]);

        // Detach tracker
        self.tree.freed = null;

        // Reset the main tracker
        self.tracker.reset();
    }

    /// Load reusable pages from the FreeDB into the FileManager.
    /// Only reclaims entries where entry_tx_id <= oldest_active_reader.
    /// Takes the oldest entry first.
    pub fn loadReusablePages(self: *FreePageManager) !void {
        if (self.tree.root == 0) return;

        // Seek to the first (oldest) entry
        var it = self.tree.seekFirst();
        const entry = it.next() orelse return;

        // Check reader safety
        const entry_tx_id = decodeFreeKey(entry.key[0..8]);
        if (entry_tx_id > self.oldest_active_reader) return;

        // Decode page IDs
        var out: [MAX_TRACKED_PAGES]u64 = undefined;
        const pages = decodeFreeValue(entry.value, &out);

        // Load into FileManager
        self.tree.fm.loadReusablePages(pages);

        // Delete this entry from FreeDB
        self.tree.freed = &self.carry_forward;
        try self.tree.delete(&entry.key[0..8].*);
        self.tree.freed = null;
    }

    /// Return the current FreeDB root page for the meta page.
    pub fn freeRoot(self: *const FreePageManager) u64 {
        return self.tree.root;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "tracker add/reset/get" {
    var tracker = FreePageTracker{};
    try testing.expectEqual(@as(u32, 0), tracker.count);

    tracker.addPage(10);
    tracker.addPage(20);
    tracker.addPage(30);
    try testing.expectEqual(@as(u32, 3), tracker.count);

    const pages = tracker.getFreedPages();
    try testing.expectEqual(@as(usize, 3), pages.len);
    try testing.expectEqual(@as(u64, 10), pages[0]);
    try testing.expectEqual(@as(u64, 20), pages[1]);
    try testing.expectEqual(@as(u64, 30), pages[2]);

    tracker.reset();
    try testing.expectEqual(@as(u32, 0), tracker.count);
    try testing.expectEqual(@as(usize, 0), tracker.getFreedPages().len);
}

test "tracker overflow does not crash" {
    var tracker = FreePageTracker{};
    for (0..MAX_TRACKED_PAGES + 10) |i| {
        tracker.addPage(@intCast(i));
    }
    try testing.expectEqual(@as(u32, MAX_TRACKED_PAGES), tracker.count);
}

test "FreeDB key encoding roundtrip" {
    var buf: [8]u8 = undefined;
    const cases = [_]u64{ 0, 1, 42, 1000, math.maxInt(u64) };
    for (cases) |tx| {
        encodeFreeKey(&buf, tx);
        try testing.expectEqual(tx, decodeFreeKey(&buf));
    }
}

test "FreeDB value encoding roundtrip" {
    const pages = [_]u64{ 5, 10, 15, 20, 25 };
    var buf: [4 + 5 * 8]u8 = undefined;
    const written = encodeFreeValue(&buf, &pages);
    try testing.expectEqual(@as(usize, 44), written);

    var out: [10]u64 = undefined;
    const decoded = decodeFreeValue(buf[0..written], &out);
    try testing.expectEqual(@as(usize, 5), decoded.len);
    for (pages, 0..) |pg, i| {
        try testing.expectEqual(pg, decoded[i]);
    }
}

test "FreeDB key comparison ordering" {
    var k1: [8]u8 = undefined;
    var k2: [8]u8 = undefined;
    var k3: [8]u8 = undefined;

    encodeFreeKey(&k1, 1);
    encodeFreeKey(&k2, 2);
    encodeFreeKey(&k3, 100);

    try testing.expectEqual(math.Order.lt, freeDbKeyCmp(&k1, &k2));
    try testing.expectEqual(math.Order.lt, freeDbKeyCmp(&k2, &k3));
    try testing.expectEqual(math.Order.gt, freeDbKeyCmp(&k3, &k1));
    try testing.expectEqual(math.Order.eq, freeDbKeyCmp(&k1, &k1));
}

test "commit freed pages then read back via FreeDB" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var fpm = FreePageManager.init(0, &fm);

    // Simulate freed pages
    fpm.tracker.addPage(10);
    fpm.tracker.addPage(20);
    fpm.tracker.addPage(30);

    try fpm.commitFreedPages(1);

    // FreeDB should now have root != 0
    try testing.expect(fpm.freeRoot() != 0);

    // Lookup the entry
    var key_buf: [8]u8 = undefined;
    encodeFreeKey(&key_buf, 1);
    const val = fpm.tree.lookup(&key_buf);
    try testing.expect(val != null);

    // Decode and verify
    var out: [10]u64 = undefined;
    const pages = decodeFreeValue(val.?, &out);
    try testing.expectEqual(@as(usize, 3), pages.len);
    try testing.expectEqual(@as(u64, 10), pages[0]);
    try testing.expectEqual(@as(u64, 20), pages[1]);
    try testing.expectEqual(@as(u64, 30), pages[2]);
}

test "freed pages are reused" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{ .page_size = 256 });
    defer fm.close();

    var fpm = FreePageManager.init(0, &fm);

    // Build a tree and insert keys to create some pages
    var tree = btree.BPlusTree.init(0, &fm, freeDbKeyCmp);
    tree.freed = &fpm.tracker;

    // Insert enough keys to create pages
    var key_buf: [8]u8 = undefined;
    for (0..10) |i| {
        encodeFreeKey(&key_buf, @intCast(i));
        try tree.insert(&key_buf, &key_buf);
    }

    // Now delete some keys — COW will free old pages
    fpm.tracker.reset();
    for (0..5) |i| {
        encodeFreeKey(&key_buf, @intCast(i));
        try tree.delete(&key_buf);
    }

    const freed_count = fpm.tracker.count;
    try testing.expect(freed_count > 0);

    // Commit freed pages to FreeDB
    tree.freed = null;
    try fpm.commitFreedPages(1);

    // Load reusable pages
    try fpm.loadReusablePages();
    try testing.expect(fm.reuse_count > 0);

    // Record file size before new inserts
    const size_before = fm.file_size;

    // Insert more entries — should reuse pages, not grow file
    tree.freed = null;
    var reuse_used: u32 = 0;
    for (100..105) |i| {
        encodeFreeKey(&key_buf, @intCast(i));
        const before = fm.reuse_pos;
        try tree.insert(&key_buf, &key_buf);
        if (fm.reuse_pos > before) {
            reuse_used += fm.reuse_pos - before;
        }
    }

    // At least some allocations should have come from reuse
    try testing.expect(reuse_used > 0 or fm.file_size == size_before);
}

test "active reader prevents reclamation" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    // Pre-allocate pages so the file covers the IDs we'll free
    var real_pages: [2]u64 = undefined;
    for (&real_pages) |*p| {
        p.* = try fm.allocPage();
    }
    try fm.remap();

    var fpm = FreePageManager.init(0, &fm);

    // Free pre-allocated pages at tx=6
    fpm.tracker.addPage(real_pages[0]);
    fpm.tracker.addPage(real_pages[1]);
    try fpm.commitFreedPages(6);

    // Set oldest_reader = 5 (before tx=6)
    fpm.oldest_active_reader = 5;
    try fpm.loadReusablePages();
    try testing.expectEqual(@as(u32, 0), fm.reuse_count);

    // Clear reader constraint
    fpm.oldest_active_reader = math.maxInt(u64);
    try fpm.loadReusablePages();
    try testing.expect(fm.reuse_count > 0);
}

test "multiple commits create multiple FreeDB entries; loadReusable takes oldest first" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    // Pre-allocate many pages so the file covers all IDs we'll free and reuse
    var real_pages: [30]u64 = undefined;
    for (&real_pages) |*p| {
        p.* = try fm.allocPage();
    }
    try fm.remap();

    var fpm = FreePageManager.init(0, &fm);

    // Commit at tx=1 with one pre-allocated page
    fpm.tracker.addPage(real_pages[0]);
    try fpm.commitFreedPages(1);

    // Commit at tx=2
    fpm.tracker.addPage(real_pages[1]);
    try fpm.commitFreedPages(2);

    // Commit at tx=3
    fpm.tracker.addPage(real_pages[2]);
    try fpm.commitFreedPages(3);

    // Load should get tx=1 entry first
    try fpm.loadReusablePages();
    try testing.expect(fm.reuse_count > 0);
    try testing.expectEqual(real_pages[0], fm.reuse_pages[0]);

    fm.clearReusablePages();

    // Load again should get tx=2 entry
    try fpm.loadReusablePages();
    try testing.expect(fm.reuse_count > 0);
    try testing.expectEqual(real_pages[1], fm.reuse_pages[0]);

    fm.clearReusablePages();

    // Load again should get tx=3 entry
    try fpm.loadReusablePages();
    try testing.expect(fm.reuse_count > 0);
    // tx=3 value has [real_pages[2], carry_forward_pages...], first should be our page
    try testing.expectEqual(real_pages[2], fm.reuse_pages[0]);
}

test "carry-forward: FreeDB COW pages appear in carry_forward" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    var fpm = FreePageManager.init(0, &fm);

    // First commit creates the FreeDB tree (no COW pages yet since tree is empty)
    fpm.tracker.addPage(100);
    try fpm.commitFreedPages(1);

    // Second commit — FreeDB insert will COW the existing leaf,
    // so carry_forward should capture the old FreeDB page
    fpm.tracker.addPage(200);
    try fpm.commitFreedPages(2);

    // carry_forward should have at least one page (the old FreeDB leaf)
    try testing.expect(fpm.carry_forward.count > 0);
}
