// ZatDB — File manager with mmap-backed reads and pwrite-based writes
//
// Manages a single .zat database file. Provides:
// - Read-only mmap for zero-copy page reads
// - pwrite for crash-safe page writes
// - Dual meta-page commit protocol (see meta.zig)

const std = @import("std");
const posix = std.posix;
const meta = @import("meta.zig");
const encoding = @import("encoding.zig");
const page = @import("page.zig");
const testing = std.testing;

pub const OpenOpts = struct {
    page_size: u32 = 0, // 0 = auto-detect OS page size
};

pub const FileManager = struct {
    file: std.fs.File,
    map: ?[]align(std.heap.page_size_min) u8,
    page_size: u32,
    file_size: u64,
    next_page: u64,
    reuse_pages: [256]u64 = .{0} ** 256,
    reuse_count: u32 = 0,
    reuse_pos: u32 = 0,

    /// Open or create a .zat database file.
    pub fn open(dir: std.fs.Dir, name: []const u8, opts: OpenOpts) !FileManager {
        const file = dir.openFile(name, .{ .mode = .read_write }) catch |err| {
            if (err == error.FileNotFound) {
                return create(dir, name, opts);
            }
            return err;
        };
        return openExisting(file);
    }

    fn create(dir: std.fs.Dir, name: []const u8, opts: OpenOpts) !FileManager {
        const ps: u32 = if (opts.page_size != 0) opts.page_size else @intCast(std.heap.pageSize());
        const file = try dir.createFile(name, .{ .read = true });
        errdefer file.close();

        const file_size: u64 = @as(u64, ps) * 2;
        try file.setEndPos(file_size);

        // Write initial meta to slot 0
        var buf: [meta.META_HEADER_SIZE]u8 = undefined;
        meta.writeMeta(&buf, .{
            .page_size = ps,
            .tx_id = 0,
            .next_page = 2,
        });
        try file.pwriteAll(&buf, 0);

        // mmap
        const map = try posix.mmap(
            null,
            @intCast(file_size),
            std.c.PROT.READ,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        return .{
            .file = file,
            .map = map,
            .page_size = ps,
            .file_size = file_size,
            .next_page = 2,
        };
    }

    fn openExisting(file: std.fs.File) !FileManager {
        errdefer file.close();
        const file_size = try file.getEndPos();

        const map = try posix.mmap(
            null,
            @intCast(file_size),
            std.c.PROT.READ,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );
        errdefer posix.munmap(map);

        // Read page_size from offset 8 (within first meta page header)
        const ps = encoding.readU32(map[8..12]);
        const ps_usize: usize = ps;

        if (ps_usize == 0 or @as(u64, ps_usize) * 2 > file_size) return error.CorruptDatabase;

        // Validate meta pages
        const active = meta.pickActiveMeta(map[0..ps_usize], map[ps_usize .. ps_usize * 2]) orelse
            return error.CorruptDatabase;

        return .{
            .file = file,
            .map = map,
            .page_size = ps,
            .file_size = file_size,
            .next_page = active.next_page,
        };
    }

    /// Read a page via mmap. Returns a slice into the mapped region.
    pub fn readPage(self: *const FileManager, page_id: u64) []const u8 {
        const offset: usize = @intCast(page_id * @as(u64, self.page_size));
        const ps: usize = self.page_size;
        return self.map.?[offset..][0..ps];
    }

    /// Write a full page via pwrite.
    pub fn writePage(self: *FileManager, page_id: u64, data: []const u8) !void {
        std.debug.assert(data.len == self.page_size);
        const offset: u64 = page_id * @as(u64, self.page_size);
        try self.file.pwriteAll(data, offset);
    }

    /// Allocate a new page. Returns a reusable page if available, otherwise extends the file.
    /// Call remap() before reading the new page via mmap.
    pub fn allocPage(self: *FileManager) !u64 {
        if (self.reuse_pos < self.reuse_count) {
            const page_id = self.reuse_pages[self.reuse_pos];
            self.reuse_pos += 1;
            return page_id;
        }
        const page_id = self.next_page;
        self.next_page += 1;
        const new_size: u64 = self.next_page * @as(u64, self.page_size);
        try self.file.setEndPos(new_size);
        self.file_size = new_size;
        return page_id;
    }

    /// Load reusable page IDs for allocation. Replaces any existing reuse list.
    pub fn loadReusablePages(self: *FileManager, pages: []const u64) void {
        const count: u32 = @intCast(@min(pages.len, self.reuse_pages.len));
        @memcpy(self.reuse_pages[0..count], pages[0..count]);
        self.reuse_count = count;
        self.reuse_pos = 0;
    }

    /// Clear the reusable pages list.
    pub fn clearReusablePages(self: *FileManager) void {
        self.reuse_count = 0;
        self.reuse_pos = 0;
    }

    /// Re-mmap the file (e.g., after allocating new pages).
    pub fn remap(self: *FileManager) !void {
        if (self.map) |m| posix.munmap(m);
        self.map = try posix.mmap(
            null,
            @intCast(self.file_size),
            std.c.PROT.READ,
            .{ .TYPE = .SHARED },
            self.file.handle,
            0,
        );
    }

    /// fsync the file.
    pub fn sync(self: *FileManager) !void {
        try self.file.sync();
    }

    /// Write a meta page to the appropriate slot (dual-page protocol).
    pub fn commitMeta(self: *FileManager, m: meta.MetaPage) !void {
        const ps: usize = self.page_size;
        const map = self.map orelse return error.NotMapped;
        const slot0_valid = meta.isValidMeta(map[0..ps]);
        const slot1_valid = meta.isValidMeta(map[ps .. ps * 2]);

        const target_slot: u64 = blk: {
            if (!slot0_valid and !slot1_valid) break :blk 0;
            if (!slot1_valid) break :blk 1;
            if (!slot0_valid) break :blk 0;
            // Both valid: overwrite slot with lower tx_id
            const m0 = meta.readMeta(map[0..ps]);
            const m1 = meta.readMeta(map[ps .. ps * 2]);
            break :blk if (m0.tx_id <= m1.tx_id) @as(u64, 0) else 1;
        };

        var buf: [meta.META_HEADER_SIZE]u8 = undefined;
        meta.writeMeta(&buf, m);
        try self.file.pwriteAll(&buf, target_slot * @as(u64, self.page_size));
    }

    /// Read the active (highest tx_id, valid) meta page.
    pub fn readActiveMeta(self: *const FileManager) !meta.MetaPage {
        const ps: usize = self.page_size;
        const map = self.map orelse return error.NotMapped;
        return meta.pickActiveMeta(map[0..ps], map[ps .. ps * 2]) orelse
            return error.CorruptDatabase;
    }

    /// Close the file manager (unmap + close file).
    pub fn close(self: *FileManager) void {
        if (self.map) |m| posix.munmap(m);
        self.map = null;
        self.file.close();
    }
};

// ============================================================================
// Tests
// ============================================================================

// --- Phase C: FileManager Creation & Basic I/O ---

test "subtask 6: create new .zat file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const ps: u64 = fm.page_size;
    try testing.expectEqual(ps * 2, fm.file_size);
    try testing.expectEqual(@as(u64, 2), fm.next_page);

    // Meta slot 0 should be valid
    const map = fm.map.?;
    const ps_usize: usize = fm.page_size;
    try testing.expect(meta.isValidMeta(map[0..ps_usize]));

    // Read back meta from slot 0
    const m = meta.readMeta(map[0..ps_usize]);
    try testing.expectEqual(meta.MAGIC, m.magic);
    try testing.expectEqual(@as(u64, 0), m.tx_id);
    try testing.expectEqual(@as(u64, 2), m.next_page);

    // Meta slot 1 should be invalid (all zeros)
    try testing.expect(!meta.isValidMeta(map[ps_usize .. ps_usize * 2]));
}

test "subtask 7: allocPage, writePage, remap, readPage" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const ps: usize = fm.page_size;
    const page_id = try fm.allocPage();
    try testing.expectEqual(@as(u64, 2), page_id);

    // Write data
    var write_buf: [65536]u8 = undefined;
    const buf = write_buf[0..ps];
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "hello", "world");
    try fm.writePage(page_id, buf);

    // Must remap after allocPage
    try fm.remap();

    // Read back via mmap
    const read_data = fm.readPage(page_id);
    try testing.expectEqualStrings("hello", page.leafGetKey(read_data, 0));
    try testing.expectEqualStrings("world", page.leafGetValue(read_data, 0));
}

test "subtask 8: allocate 3 pages sequentially" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const ps: u64 = fm.page_size;
    const initial_size = fm.file_size;

    const p1 = try fm.allocPage();
    try testing.expectEqual(@as(u64, 2), p1);
    try testing.expectEqual(initial_size + ps, fm.file_size);

    const p2 = try fm.allocPage();
    try testing.expectEqual(@as(u64, 3), p2);
    try testing.expectEqual(initial_size + ps * 2, fm.file_size);

    const p3 = try fm.allocPage();
    try testing.expectEqual(@as(u64, 4), p3);
    try testing.expectEqual(initial_size + ps * 3, fm.file_size);
}

test "subtask 9: write + sync (smoke test)" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const ps: usize = fm.page_size;
    const page_id = try fm.allocPage();

    var write_buf: [65536]u8 = undefined;
    const buf = write_buf[0..ps];
    @memset(buf, 0xAB);
    try fm.writePage(page_id, buf);
    try fm.sync();
}

// --- Phase D: Meta Commit Protocol ---

test "subtask 10: fresh DB commit tx_id=1 → slot 1" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    try fm.commitMeta(.{
        .page_size = fm.page_size,
        .tx_id = 1,
        .next_page = 2,
    });

    // Slot 1 should now be valid with tx_id=1
    const ps: usize = fm.page_size;
    const map = fm.map.?;
    try testing.expect(meta.isValidMeta(map[ps .. ps * 2]));
    const m1 = meta.readMeta(map[ps .. ps * 2]);
    try testing.expectEqual(@as(u64, 1), m1.tx_id);

    // Slot 0 still has tx_id=0
    const m0 = meta.readMeta(map[0..ps]);
    try testing.expectEqual(@as(u64, 0), m0.tx_id);
}

test "subtask 11: two commits alternate slots" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const ps: usize = fm.page_size;
    const map = fm.map.?;

    // Commit 1 → slot 1
    try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 1, .next_page = 2 });
    try testing.expectEqual(@as(u64, 1), meta.readMeta(map[ps .. ps * 2]).tx_id);
    try testing.expectEqual(@as(u64, 0), meta.readMeta(map[0..ps]).tx_id);

    // Commit 2 → slot 0 (overwrites tx_id=0)
    try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 2, .next_page = 2 });
    try testing.expectEqual(@as(u64, 2), meta.readMeta(map[0..ps]).tx_id);
    try testing.expectEqual(@as(u64, 1), meta.readMeta(map[ps .. ps * 2]).tx_id);
}

test "subtask 12: readActiveMeta returns highest tx_id" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 5, .next_page = 2 });
    try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 10, .next_page = 2 });

    const active = try fm.readActiveMeta();
    try testing.expectEqual(@as(u64, 10), active.tx_id);
}

// --- Phase E: Reopen & Recovery ---

test "subtask 13: create, commit, close, reopen → recovery" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();
        try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 3, .next_page = 5 });
    }

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();
        const active = try fm.readActiveMeta();
        try testing.expectEqual(@as(u64, 3), active.tx_id);
        try testing.expectEqual(@as(u64, 5), fm.next_page);
    }
}

test "subtask 14: two commits, close, reopen → picks higher tx_id" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();
        try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 1, .next_page = 3 });
        try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 2, .next_page = 4 });
    }

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();
        const active = try fm.readActiveMeta();
        try testing.expectEqual(@as(u64, 2), active.tx_id);
        try testing.expectEqual(@as(u64, 4), fm.next_page);
    }
}

test "subtask 15: corrupt higher-tx_id meta → falls back to other" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();
        // After these two commits: slot 0 has tx_id=2, slot 1 has tx_id=1
        try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 1, .next_page = 3 });
        try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 2, .next_page = 4 });
    }

    // Corrupt slot 0 (which has tx_id=2, the higher one) — corrupt magic byte
    {
        const file = try tmp.dir.openFile("test.zat", .{ .mode = .read_write });
        defer file.close();
        try file.pwriteAll(&[_]u8{0xFF}, 0);
    }

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();
        const active = try fm.readActiveMeta();
        try testing.expectEqual(@as(u64, 1), active.tx_id);
    }
}

test "subtask 16: corrupt both meta pages → CorruptDatabase" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const ps: u32 = blk: {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();
        try fm.commitMeta(.{ .page_size = fm.page_size, .tx_id = 1, .next_page = 3 });
        break :blk fm.page_size;
    };

    // Corrupt both slots
    {
        const file = try tmp.dir.openFile("test.zat", .{ .mode = .read_write });
        defer file.close();
        try file.pwriteAll(&[_]u8{0xFF}, 0); // corrupt slot 0
        try file.pwriteAll(&[_]u8{0xFF}, @as(u64, ps)); // corrupt slot 1
    }

    const result = FileManager.open(tmp.dir, "test.zat", .{});
    try testing.expectError(error.CorruptDatabase, result);
}

// --- Phase F: mmap Integration ---

test "subtask 17: write leaf via page.zig, remap, read back" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const ps: usize = fm.page_size;
    const page_id = try fm.allocPage();

    var write_buf: [65536]u8 = undefined;
    const buf = write_buf[0..ps];
    page.initLeaf(buf, .eav);
    try page.leafInsertEntry(buf, 0, "key1", "val1");
    try page.leafInsertEntry(buf, 1, "key2", "val2");
    try page.leafInsertEntry(buf, 2, "key3", "val3");
    try fm.writePage(page_id, buf);

    try fm.remap();

    const read_data = fm.readPage(page_id);
    try testing.expectEqual(@as(u16, 3), page.leafEntryCount(read_data));
    try testing.expectEqualStrings("key1", page.leafGetKey(read_data, 0));
    try testing.expectEqualStrings("val2", page.leafGetValue(read_data, 1));
    try testing.expectEqualStrings("key3", page.leafGetKey(read_data, 2));
}

test "subtask 18: write 5 pages, remap once, read all" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const ps: usize = fm.page_size;
    var page_ids: [5]u64 = undefined;

    // Allocate and write 5 pages
    for (0..5) |i| {
        page_ids[i] = try fm.allocPage();
        var write_buf: [65536]u8 = undefined;
        const buf = write_buf[0..ps];
        page.initLeaf(buf, .eav);
        var key_buf: [8]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "key{d}", .{i}) catch unreachable;
        var val_buf: [8]u8 = undefined;
        const val = std.fmt.bufPrint(&val_buf, "val{d}", .{i}) catch unreachable;
        try page.leafInsertEntry(buf, 0, key, val);
        try fm.writePage(page_ids[i], buf);
    }

    // Single remap
    try fm.remap();

    // Read all back
    for (0..5) |i| {
        const read_data = fm.readPage(page_ids[i]);
        try testing.expectEqual(@as(u16, 1), page.leafEntryCount(read_data));
        var expected_key: [8]u8 = undefined;
        const ek = std.fmt.bufPrint(&expected_key, "key{d}", .{i}) catch unreachable;
        try testing.expectEqualStrings(ek, page.leafGetKey(read_data, 0));
    }
}

// --- Phase G: Full Commit Protocol ---

test "subtask 19: full commit protocol → reopen recovers" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();

        const ps: usize = fm.page_size;
        const page_id = try fm.allocPage();

        // Write data page
        var write_buf: [65536]u8 = undefined;
        const buf = write_buf[0..ps];
        page.initLeaf(buf, .eav);
        try page.leafInsertEntry(buf, 0, "persist", "data");
        try fm.writePage(page_id, buf);

        // Sync data before meta commit
        try fm.sync();

        // Commit meta
        try fm.commitMeta(.{
            .page_size = fm.page_size,
            .tx_id = 1,
            .eav_root_page = page_id,
            .next_page = fm.next_page,
        });
    }

    // Reopen and verify
    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();

        const active = try fm.readActiveMeta();
        try testing.expectEqual(@as(u64, 1), active.tx_id);

        // Read the data page back
        const read_data = fm.readPage(active.eav_root_page);
        try testing.expectEqualStrings("persist", page.leafGetKey(read_data, 0));
        try testing.expectEqualStrings("data", page.leafGetValue(read_data, 0));
    }
}

test "subtask 20: two full transactions → reopen recovers latest" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();

        const ps: usize = fm.page_size;

        // Transaction 1
        const p1 = try fm.allocPage();
        var write_buf: [65536]u8 = undefined;
        var buf = write_buf[0..ps];
        page.initLeaf(buf, .eav);
        try page.leafInsertEntry(buf, 0, "tx1_key", "tx1_val");
        try fm.writePage(p1, buf);
        try fm.sync();
        try fm.commitMeta(.{
            .page_size = fm.page_size,
            .tx_id = 1,
            .eav_root_page = p1,
            .next_page = fm.next_page,
        });

        // Transaction 2
        const p2 = try fm.allocPage();
        buf = write_buf[0..ps];
        page.initLeaf(buf, .eav);
        try page.leafInsertEntry(buf, 0, "tx2_key", "tx2_val");
        try fm.writePage(p2, buf);
        try fm.sync();
        try fm.commitMeta(.{
            .page_size = fm.page_size,
            .tx_id = 2,
            .eav_root_page = p2,
            .next_page = fm.next_page,
        });
    }

    // Reopen and verify latest transaction
    {
        var fm = try FileManager.open(tmp.dir, "test.zat", .{});
        defer fm.close();

        const active = try fm.readActiveMeta();
        try testing.expectEqual(@as(u64, 2), active.tx_id);

        const read_data = fm.readPage(active.eav_root_page);
        try testing.expectEqualStrings("tx2_key", page.leafGetKey(read_data, 0));
        try testing.expectEqualStrings("tx2_val", page.leafGetValue(read_data, 0));
    }
}

// --- Phase H: Reusable Pages ---

test "allocPage with reusable pages returns reuse first, then extends" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var fm = try FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    // Allocate a few pages to grow the file
    _ = try fm.allocPage(); // page 2
    _ = try fm.allocPage(); // page 3
    _ = try fm.allocPage(); // page 4
    const size_before = fm.file_size;
    const next_before = fm.next_page; // should be 5

    // Load reusable pages
    fm.loadReusablePages(&[_]u64{ 3, 4 });

    // Next allocs should return reuse pages without growing file
    const r1 = try fm.allocPage();
    try testing.expectEqual(@as(u64, 3), r1);
    try testing.expectEqual(size_before, fm.file_size);

    const r2 = try fm.allocPage();
    try testing.expectEqual(@as(u64, 4), r2);
    try testing.expectEqual(size_before, fm.file_size);

    // Reuse exhausted — next alloc should extend
    const r3 = try fm.allocPage();
    try testing.expectEqual(next_before, r3);
    try testing.expect(fm.file_size > size_before);

    // clearReusablePages resets the reuse list
    fm.loadReusablePages(&[_]u64{ 10, 20 });
    fm.clearReusablePages();
    const r4 = try fm.allocPage();
    try testing.expectEqual(next_before + 1, r4);
}
