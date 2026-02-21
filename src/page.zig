// ZatDB — Slotted page primitives
//
// Operates on raw byte buffers — no files, no allocators. The caller provides
// a []u8 slice and this module reads/writes structured data within it.
//
// Slotted page design: entry offsets grow from the header area downward (toward
// higher addresses), entry data grows from the end of the page upward (toward
// lower addresses). When they collide, the page is full.
//
// Layout (leaf page):
//   [header: 24 bytes][offset0: 2][offset1: 2]...  -->  <-- ...[entry1][entry0]
//
// Layout (branch page):
//   [header: 16 bytes][offset0: 2][offset1: 2]...  -->  <-- ...[entry1][entry0]

const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;
const encoding = @import("encoding.zig");

// ============================================================================
// Types & Constants
// ============================================================================

pub const PageType = enum(u8) {
    branch = 0x01,
    leaf = 0x02,
    overflow = 0x03,
    free = 0x04,
};

pub const IndexId = enum(u8) {
    eav = 0,
    ave = 1,
    vae = 2,
    tx_log = 3,
    free = 4,
};

pub const Header = struct {
    page_type: PageType,
    index_id: IndexId,
    num_entries: u16,
};

pub const KeyCompareFn = *const fn ([]const u8, []const u8) math.Order;

pub const PageError = error{ PageFull, InvalidPageType, InvalidOffset };

pub const SplitResult = struct {
    separator_key: []const u8,
};

pub const BranchSplitResult = struct {
    separator_key: []const u8,
    left_right_child: u64,
};

/// Size of the offset entries in the slot array.
pub const OFFSET_SIZE: u16 = 2;

// Header layout:
//   Leaf:   [page_type: 1][index_id: 1][num_entries: 2][reserved: 4][prev_page: 8][next_page: 8] = 24
//   Branch: [page_type: 1][index_id: 1][num_entries: 2][reserved: 4][right_child: 8] = 16
pub const LEAF_HEADER_SIZE: u16 = 24;
pub const BRANCH_HEADER_SIZE: u16 = 16;

// Overflow header: [page_type: 1][index_id: 1][reserved: 2][next_page: 8][data_len: 4][reserved: 4] = 20
pub const OVERFLOW_HEADER_SIZE: u16 = 20;

// ============================================================================
// Header read/write
// ============================================================================

pub fn writeHeader(buf: []u8, header: Header) void {
    buf[0] = @intFromEnum(header.page_type);
    buf[1] = @intFromEnum(header.index_id);
    encoding.writeU16(buf[2..4], header.num_entries);
}

pub fn readHeader(buf: []const u8) Header {
    return .{
        .page_type = @enumFromInt(buf[0]),
        .index_id = @enumFromInt(buf[1]),
        .num_entries = encoding.readU16(buf[2..4]),
    };
}

// ============================================================================
// Leaf page operations
// ============================================================================

pub fn initLeaf(buf: []u8, index_id: IndexId) void {
    @memset(buf, 0);
    writeHeader(buf, .{ .page_type = .leaf, .index_id = index_id, .num_entries = 0 });
}

pub fn getLeafPrevPage(buf: []const u8) u64 {
    return encoding.readU64(buf[8..16]);
}

pub fn setLeafPrevPage(buf: []u8, page_id: u64) void {
    encoding.writeU64(buf[8..16], page_id);
}

pub fn getLeafNextPage(buf: []const u8) u64 {
    return encoding.readU64(buf[16..24]);
}

pub fn setLeafNextPage(buf: []u8, page_id: u64) void {
    encoding.writeU64(buf[16..24], page_id);
}

/// Returns the free space available for new entries (data + offset slot).
pub fn leafFreeSpace(buf: []const u8) u16 {
    const header = readHeader(buf);
    const slot_end: u16 = LEAF_HEADER_SIZE + header.num_entries * OFFSET_SIZE;
    const min_offset = leafMinEntryOffset(buf, header.num_entries);
    if (min_offset <= slot_end) return 0;
    return min_offset - slot_end;
}

/// Returns the lowest entry offset (or page size if no entries).
fn leafMinEntryOffset(buf: []const u8, num_entries: u16) u16 {
    if (num_entries == 0) return @intCast(buf.len);
    var min: u16 = @intCast(buf.len);
    for (0..num_entries) |i| {
        const off = getSlotOffset(buf, LEAF_HEADER_SIZE, @intCast(i));
        if (off < min) min = off;
    }
    return min;
}

/// Get the offset stored in slot `idx`.
fn getSlotOffset(buf: []const u8, header_size: u16, idx: u16) u16 {
    const slot_pos = header_size + idx * OFFSET_SIZE;
    return encoding.readU16(buf[slot_pos..][0..2]);
}

/// Set the offset stored in slot `idx`.
fn setSlotOffset(buf: []u8, header_size: u16, idx: u16, offset: u16) void {
    const slot_pos = header_size + idx * OFFSET_SIZE;
    encoding.writeU16(buf[slot_pos..][0..2], offset);
}

/// Insert a key-value entry at position `idx` in a leaf page.
/// Entry format: [key_len: 2][key_data][val_len: 2][val_data]
pub fn leafInsertEntry(buf: []u8, idx: u16, key: []const u8, value: []const u8) PageError!void {
    var header = readHeader(buf);
    const entry_size: u16 = @intCast(2 + key.len + 2 + value.len);

    // Check free space: need room for entry data + one new offset slot
    const slot_end = LEAF_HEADER_SIZE + (header.num_entries + 1) * OFFSET_SIZE;
    const min_offset = leafMinEntryOffset(buf, header.num_entries);
    if (min_offset < slot_end or (min_offset - slot_end) < entry_size) {
        return PageError.PageFull;
    }

    // Write entry data at the end (growing upward)
    const entry_offset: u16 = min_offset - entry_size;
    writeLeafEntry(buf, entry_offset, key, value);

    // Shift slot offsets to make room at idx
    var i: u16 = header.num_entries;
    while (i > idx) : (i -= 1) {
        const prev_off = getSlotOffset(buf, LEAF_HEADER_SIZE, i - 1);
        setSlotOffset(buf, LEAF_HEADER_SIZE, i, prev_off);
    }
    setSlotOffset(buf, LEAF_HEADER_SIZE, idx, entry_offset);

    header.num_entries += 1;
    writeHeader(buf, header);
}

fn writeLeafEntry(buf: []u8, offset: u16, key: []const u8, value: []const u8) void {
    var pos: usize = offset;
    encoding.writeU16(buf[pos..][0..2], @intCast(key.len));
    pos += 2;
    @memcpy(buf[pos..][0..key.len], key);
    pos += key.len;
    encoding.writeU16(buf[pos..][0..2], @intCast(value.len));
    pos += 2;
    @memcpy(buf[pos..][0..value.len], value);
}

/// Get the key of entry at slot `idx` in a leaf page.
pub fn leafGetKey(buf: []const u8, idx: u16) []const u8 {
    const offset = getSlotOffset(buf, LEAF_HEADER_SIZE, idx);
    const key_len = encoding.readU16(buf[offset..][0..2]);
    return buf[offset + 2 ..][0..key_len];
}

/// Get the value of entry at slot `idx` in a leaf page.
pub fn leafGetValue(buf: []const u8, idx: u16) []const u8 {
    const offset = getSlotOffset(buf, LEAF_HEADER_SIZE, idx);
    const key_len = encoding.readU16(buf[offset..][0..2]);
    const val_pos = offset + 2 + key_len;
    const val_len = encoding.readU16(buf[val_pos..][0..2]);
    return buf[val_pos + 2 ..][0..val_len];
}

/// Return the number of entries in a leaf page.
pub fn leafEntryCount(buf: []const u8) u16 {
    return readHeader(buf).num_entries;
}

/// Binary search for an exact key match. Returns the slot index or null.
pub fn leafFindKey(buf: []const u8, key: []const u8, cmp: KeyCompareFn) ?u16 {
    const count = leafEntryCount(buf);
    var lo: u16 = 0;
    var hi: u16 = count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const entry_key = leafGetKey(buf, mid);
        switch (cmp(entry_key, key)) {
            .lt => lo = mid + 1,
            .gt => hi = mid,
            .eq => return mid,
        }
    }
    return null;
}

/// Lower-bound binary search: returns the insertion point for `key`.
/// All keys at indices < result compare less than `key`.
pub fn leafSearchPoint(buf: []const u8, key: []const u8, cmp: KeyCompareFn) u16 {
    const count = leafEntryCount(buf);
    var lo: u16 = 0;
    var hi: u16 = count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const entry_key = leafGetKey(buf, mid);
        switch (cmp(entry_key, key)) {
            .lt => lo = mid + 1,
            .gt, .eq => hi = mid,
        }
    }
    return lo;
}

/// Split a leaf page. Moves the upper half of entries to `right_buf`.
/// Returns the separator key (first key in the right page) — the slice
/// points into `right_buf`.
pub fn leafSplit(buf: []u8, right_buf: []u8) SplitResult {
    const count = leafEntryCount(buf);
    const mid = count / 2;

    // Read sibling pointers before we wipe right_buf
    const header = readHeader(buf);
    const old_next = getLeafNextPage(buf);

    // Init right page with same index_id
    initLeaf(right_buf, header.index_id);

    // Copy upper half [mid..count) to right page
    var ri: u16 = 0;
    for (mid..count) |i| {
        const key = leafGetKey(buf, @intCast(i));
        const val = leafGetValue(buf, @intCast(i));
        leafInsertEntry(right_buf, ri, key, val) catch unreachable;
        ri += 1;
    }

    // Rebuild left page with [0..mid)
    // We need temp storage — collect keys/values, then reinit
    // Since entries live in buf and we're rewriting buf, we copy to right first.
    // Trick: we already have entries in buf at their offsets. We'll re-pack in place.
    repackLeaf(buf, header.index_id, 0, mid);

    // Fix sibling pointers:
    // L.prev stays unchanged (already set during repack via initLeaf)
    setLeafPrevPage(buf, getLeafPrevPage(buf)); // preserved by repack
    setLeafNextPage(buf, 0); // caller will set this to right page id
    setLeafPrevPage(right_buf, 0); // caller will set this to left page id
    setLeafNextPage(right_buf, old_next);

    return .{ .separator_key = leafGetKey(right_buf, 0) };
}

/// Repack a leaf: keep only entries [start..end) and reinitialize the page.
fn repackLeaf(buf: []u8, index_id: IndexId, start: u16, end: u16) void {
    // First, collect all entries into a temp area at the top of the stack.
    // We'll use a simple approach: read keys/values from current buf, store
    // offset+lengths, then reinit and re-insert.
    const n = end - start;
    if (n == 0) {
        const prev = getLeafPrevPage(buf);
        const next = getLeafNextPage(buf);
        initLeaf(buf, index_id);
        setLeafPrevPage(buf, prev);
        setLeafNextPage(buf, next);
        return;
    }

    // Since we can't allocate, we use a two-pass approach:
    // 1. Copy all entry data to a contiguous temp region at end of buf
    //    (this works because we're re-using the same buf)
    // 2. Re-init the page and insert entries

    // Calculate total data size needed
    var total_data: usize = 0;
    for (start..end) |i| {
        const key = leafGetKey(buf, @intCast(i));
        const val = leafGetValue(buf, @intCast(i));
        total_data += 2 + key.len + 2 + val.len;
    }

    // We need a temp buffer. We can't use the same buf without corruption risk.
    // Use a stack buffer — pages are small (256-4096 typical).
    var temp: [4096 * 4]u8 = undefined;
    var temp_pos: usize = 0;

    for (start..end) |i| {
        const key = leafGetKey(buf, @intCast(i));
        const val = leafGetValue(buf, @intCast(i));
        const entry_size = 2 + key.len + 2 + val.len;
        @memcpy(temp[temp_pos..][0..2], buf[getSlotOffset(buf, LEAF_HEADER_SIZE, @intCast(i))..][0..2]);
        @memcpy(temp[temp_pos + 2 ..][0..key.len], key);
        encoding.writeU16(temp[temp_pos + 2 + key.len ..][0..2], @intCast(val.len));
        @memcpy(temp[temp_pos + 2 + key.len + 2 ..][0..val.len], val);
        temp_pos += entry_size;
    }

    // Now reinit page
    const prev = getLeafPrevPage(buf);
    const next = getLeafNextPage(buf);
    initLeaf(buf, index_id);
    setLeafPrevPage(buf, prev);
    setLeafNextPage(buf, next);

    // Re-insert entries from temp
    var read_pos: usize = 0;
    for (0..n) |i| {
        const key_len = encoding.readU16(temp[read_pos..][0..2]);
        read_pos += 2;
        const key = temp[read_pos..][0..key_len];
        read_pos += key_len;
        const val_len = encoding.readU16(temp[read_pos..][0..2]);
        read_pos += 2;
        const val = temp[read_pos..][0..val_len];
        read_pos += val_len;
        leafInsertEntry(buf, @intCast(i), key, val) catch unreachable;
    }
}

// ============================================================================
// Branch page operations
// ============================================================================

pub fn initBranch(buf: []u8, index_id: IndexId, right_child: u64) void {
    @memset(buf, 0);
    writeHeader(buf, .{ .page_type = .branch, .index_id = index_id, .num_entries = 0 });
    branchSetRightChild(buf, right_child);
}

pub fn branchGetRightChild(buf: []const u8) u64 {
    return encoding.readU64(buf[8..16]);
}

pub fn branchSetRightChild(buf: []u8, page_id: u64) void {
    encoding.writeU64(buf[8..16], page_id);
}

/// Insert a separator entry at position `idx` in a branch page.
/// Entry format: [child_page: 8][key_len: 2][key_data]
pub fn branchInsertEntry(buf: []u8, idx: u16, key: []const u8, child_page: u64) PageError!void {
    var header = readHeader(buf);
    const entry_size: u16 = @intCast(8 + 2 + key.len);

    // Check free space
    const slot_end = BRANCH_HEADER_SIZE + (header.num_entries + 1) * OFFSET_SIZE;
    const min_offset = branchMinEntryOffset(buf, header.num_entries);
    if (min_offset < slot_end or (min_offset - slot_end) < entry_size) {
        return PageError.PageFull;
    }

    // Write entry data
    const entry_offset: u16 = min_offset - entry_size;
    writeBranchEntry(buf, entry_offset, key, child_page);

    // Shift slots
    var i: u16 = header.num_entries;
    while (i > idx) : (i -= 1) {
        const prev_off = getSlotOffset(buf, BRANCH_HEADER_SIZE, i - 1);
        setSlotOffset(buf, BRANCH_HEADER_SIZE, i, prev_off);
    }
    setSlotOffset(buf, BRANCH_HEADER_SIZE, idx, entry_offset);

    header.num_entries += 1;
    writeHeader(buf, header);
}

fn branchMinEntryOffset(buf: []const u8, num_entries: u16) u16 {
    if (num_entries == 0) return @intCast(buf.len);
    var min: u16 = @intCast(buf.len);
    for (0..num_entries) |i| {
        const off = getSlotOffset(buf, BRANCH_HEADER_SIZE, @intCast(i));
        if (off < min) min = off;
    }
    return min;
}

fn writeBranchEntry(buf: []u8, offset: u16, key: []const u8, child_page: u64) void {
    var pos: usize = offset;
    encoding.writeU64(buf[pos..][0..8], child_page);
    pos += 8;
    encoding.writeU16(buf[pos..][0..2], @intCast(key.len));
    pos += 2;
    @memcpy(buf[pos..][0..key.len], key);
}

/// Get the key of entry at slot `idx` in a branch page.
pub fn branchGetKey(buf: []const u8, idx: u16) []const u8 {
    const offset = getSlotOffset(buf, BRANCH_HEADER_SIZE, idx);
    // Skip child_page (8 bytes)
    const key_len = encoding.readU16(buf[offset + 8 ..][0..2]);
    return buf[offset + 10 ..][0..key_len];
}

/// Get the child page of entry at slot `idx` in a branch page.
pub fn branchGetChild(buf: []const u8, idx: u16) u64 {
    const offset = getSlotOffset(buf, BRANCH_HEADER_SIZE, idx);
    return encoding.readU64(buf[offset..][0..8]);
}

/// Return the number of entries in a branch page.
pub fn branchEntryCount(buf: []const u8) u16 {
    return readHeader(buf).num_entries;
}

/// Find which child page a key should route to.
/// Convention: entry[i] = (child_i, key_i). child_i handles keys < key_i.
/// right_child handles keys >= last key.
pub fn branchFindChild(buf: []const u8, key: []const u8, cmp: KeyCompareFn) u64 {
    const count = branchEntryCount(buf);
    // Linear scan is fine for small branch pages; could be binary search too.
    for (0..count) |i| {
        const sep_key = branchGetKey(buf, @intCast(i));
        switch (cmp(key, sep_key)) {
            .lt => return branchGetChild(buf, @intCast(i)),
            .eq, .gt => {},
        }
    }
    return branchGetRightChild(buf);
}

/// Split a branch page. Promotes the middle key.
/// Left keeps [0..mid), left.right_child = mid's child.
/// Right gets [mid+1..N) with old right_child.
pub fn branchSplit(buf: []u8, right_buf: []u8) BranchSplitResult {
    const count = branchEntryCount(buf);
    const header = readHeader(buf);
    const mid = count / 2;

    // Save middle entry's key and child
    const mid_key = branchGetKey(buf, mid);
    const mid_child = branchGetChild(buf, mid);
    const old_right_child = branchGetRightChild(buf);

    // Copy mid_key to temp since we'll overwrite buf
    var mid_key_temp: [4096]u8 = undefined;
    @memcpy(mid_key_temp[0..mid_key.len], mid_key);
    const mid_key_saved = mid_key_temp[0..mid_key.len];

    // Init right page with old right_child
    initBranch(right_buf, header.index_id, old_right_child);

    // Copy [mid+1..count) to right page
    var ri: u16 = 0;
    for ((mid + 1)..count) |i| {
        const key = branchGetKey(buf, @intCast(i));
        const child = branchGetChild(buf, @intCast(i));
        branchInsertEntry(right_buf, ri, key, child) catch unreachable;
        ri += 1;
    }

    // Rebuild left page with [0..mid)
    repackBranch(buf, header.index_id, 0, mid, mid_child);

    // Return separator (now points into right_buf... no, we need to return
    // a stable reference). The mid key was promoted, let's point into right_buf
    // Actually the separator is the promoted key. Let's find it in the temp or
    // in the right_buf. We saved it to mid_key_temp, but that's stack.
    // Convention: return slice pointing into right_buf's first key area.
    // But the promoted key is NOT in right_buf — it's above both pages.
    // We need a different approach. Let's store the mid_key in right_buf's
    // reserved area or just accept that the caller must copy it.
    // For now: The separator_key slice points into the left page's unused area.
    // Actually the cleanest approach: write the promoted key at a known location.
    // Let's just note that the caller must use the key before modifying pages further.
    // We'll copy the mid key to the start of right_buf's unused data area.

    // Store separator key in left page's free space (between slot array and entry data).
    // After repack, left has [0..mid) entries, so there is guaranteed free space.
    const left_count = branchEntryCount(buf);
    const sep_offset = BRANCH_HEADER_SIZE + left_count * OFFSET_SIZE;
    @memcpy(buf[sep_offset..][0..mid_key_saved.len], mid_key_saved);

    return .{
        .separator_key = buf[sep_offset..][0..mid_key_saved.len],
        .left_right_child = mid_child,
    };
}

fn repackBranch(buf: []u8, index_id: IndexId, start: u16, end: u16, new_right_child: u64) void {
    const n = end - start;

    var temp: [4096 * 4]u8 = undefined;
    var temp_pos: usize = 0;

    // Save entries
    for (start..end) |i| {
        const key = branchGetKey(buf, @intCast(i));
        const child = branchGetChild(buf, @intCast(i));
        encoding.writeU64(temp[temp_pos..][0..8], child);
        temp_pos += 8;
        encoding.writeU16(temp[temp_pos..][0..2], @intCast(key.len));
        temp_pos += 2;
        @memcpy(temp[temp_pos..][0..key.len], key);
        temp_pos += key.len;
    }

    initBranch(buf, index_id, new_right_child);

    // Re-insert
    var read_pos: usize = 0;
    for (0..n) |i| {
        const child = encoding.readU64(temp[read_pos..][0..8]);
        read_pos += 8;
        const key_len = encoding.readU16(temp[read_pos..][0..2]);
        read_pos += 2;
        const key = temp[read_pos..][0..key_len];
        read_pos += key_len;
        branchInsertEntry(buf, @intCast(i), key, child) catch unreachable;
    }
}

// ============================================================================
// Overflow page operations
// ============================================================================

// Overflow header: [page_type: 1][index_id: 1][reserved: 2][next_page: 8][data_len: 4][reserved: 4] = 20

pub fn initOverflow(buf: []u8, data: []const u8, next_page: u64) void {
    @memset(buf, 0);
    writeHeader(buf, .{ .page_type = .overflow, .index_id = .free, .num_entries = 0 });
    encoding.writeU64(buf[4..12], next_page);
    encoding.writeU32(buf[12..16], @intCast(data.len));
    @memcpy(buf[OVERFLOW_HEADER_SIZE..][0..data.len], data);
}

pub fn overflowGetNextPage(buf: []const u8) u64 {
    return encoding.readU64(buf[4..12]);
}

pub fn overflowGetDataLen(buf: []const u8) u32 {
    return encoding.readU32(buf[12..16]);
}

pub fn overflowGetData(buf: []const u8) []const u8 {
    const len = overflowGetDataLen(buf);
    return buf[OVERFLOW_HEADER_SIZE..][0..len];
}

pub fn overflowCapacity(page_size: u16) u16 {
    return page_size - OVERFLOW_HEADER_SIZE;
}

// ============================================================================
// Free page operations
// ============================================================================

pub fn initFree(buf: []u8) void {
    @memset(buf, 0);
    writeHeader(buf, .{ .page_type = .free, .index_id = .free, .num_entries = 0 });
}

// ============================================================================
// Tests
// ============================================================================

// --- Phase A: Constants & Header (subtasks 1-3) ---

test "subtask 1: enums and constants" {
    // PageType values
    try testing.expectEqual(@as(u8, 0x01), @intFromEnum(PageType.branch));
    try testing.expectEqual(@as(u8, 0x02), @intFromEnum(PageType.leaf));
    try testing.expectEqual(@as(u8, 0x03), @intFromEnum(PageType.overflow));
    try testing.expectEqual(@as(u8, 0x04), @intFromEnum(PageType.free));

    // IndexId values
    try testing.expectEqual(@as(u8, 0), @intFromEnum(IndexId.eav));
    try testing.expectEqual(@as(u8, 1), @intFromEnum(IndexId.ave));
    try testing.expectEqual(@as(u8, 2), @intFromEnum(IndexId.vae));
    try testing.expectEqual(@as(u8, 3), @intFromEnum(IndexId.tx_log));
    try testing.expectEqual(@as(u8, 4), @intFromEnum(IndexId.free));

    // Header sizes
    try testing.expectEqual(@as(u16, 24), LEAF_HEADER_SIZE);
    try testing.expectEqual(@as(u16, 16), BRANCH_HEADER_SIZE);
}

test "subtask 2: header write/read" {
    var buf: [256]u8 = undefined;
    writeHeader(&buf, .{ .page_type = .leaf, .index_id = .eav, .num_entries = 42 });
    const h = readHeader(&buf);
    try testing.expectEqual(PageType.leaf, h.page_type);
    try testing.expectEqual(IndexId.eav, h.index_id);
    try testing.expectEqual(@as(u16, 42), h.num_entries);
}

test "subtask 3: header round-trip all variants" {
    var buf: [256]u8 = undefined;
    const page_types = [_]PageType{ .branch, .leaf, .overflow, .free };
    const index_ids = [_]IndexId{ .eav, .ave, .vae, .tx_log, .free };
    for (page_types) |pt| {
        for (index_ids) |iid| {
            writeHeader(&buf, .{ .page_type = pt, .index_id = iid, .num_entries = 999 });
            const h = readHeader(&buf);
            try testing.expectEqual(pt, h.page_type);
            try testing.expectEqual(iid, h.index_id);
            try testing.expectEqual(@as(u16, 999), h.num_entries);
        }
    }
}

// --- Phase B: Leaf Init & Free Space (subtasks 4-5) ---

test "subtask 4: leaf init" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);
    const h = readHeader(&buf);
    try testing.expectEqual(PageType.leaf, h.page_type);
    try testing.expectEqual(IndexId.eav, h.index_id);
    try testing.expectEqual(@as(u16, 0), h.num_entries);
    try testing.expectEqual(@as(u64, 0), getLeafPrevPage(&buf));
    try testing.expectEqual(@as(u64, 0), getLeafNextPage(&buf));

    // Test setters
    setLeafPrevPage(&buf, 10);
    setLeafNextPage(&buf, 20);
    try testing.expectEqual(@as(u64, 10), getLeafPrevPage(&buf));
    try testing.expectEqual(@as(u64, 20), getLeafNextPage(&buf));
}

test "subtask 5: leaf free space" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);
    // 256 - 24 header = 232
    try testing.expectEqual(@as(u16, 232), leafFreeSpace(&buf));
}

// --- Phase C: Leaf Insert (subtasks 6-8) ---

test "subtask 6: insert first entry" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);
    try leafInsertEntry(&buf, 0, "aaa", "hello");
    try testing.expectEqual(@as(u16, 1), leafEntryCount(&buf));
    try testing.expectEqualStrings("aaa", leafGetKey(&buf, 0));
    try testing.expectEqualStrings("hello", leafGetValue(&buf, 0));
}

test "subtask 7: insert multiple sorted" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);

    // Insert in sorted order: "ccc" at 0, "aaa" at 0 (before ccc), "bbb" at 1 (between)
    try leafInsertEntry(&buf, 0, "ccc", "v3");
    try leafInsertEntry(&buf, 0, "aaa", "v1");
    try leafInsertEntry(&buf, 1, "bbb", "v2");

    try testing.expectEqual(@as(u16, 3), leafEntryCount(&buf));
    try testing.expectEqualStrings("aaa", leafGetKey(&buf, 0));
    try testing.expectEqualStrings("bbb", leafGetKey(&buf, 1));
    try testing.expectEqualStrings("ccc", leafGetKey(&buf, 2));
    try testing.expectEqualStrings("v1", leafGetValue(&buf, 0));
    try testing.expectEqualStrings("v2", leafGetValue(&buf, 1));
    try testing.expectEqualStrings("v3", leafGetValue(&buf, 2));
}

test "subtask 8: insert until full" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);

    // Each entry: 2 + 10 + 2 + 10 = 24 bytes data + 2 bytes offset = 26 per entry
    // Available: 232 bytes. 232 / 26 = 8.9, so 8 entries should fit, 9th fails
    var i: u16 = 0;
    var key_buf: [10]u8 = undefined;
    var val_buf: [10]u8 = undefined;
    while (true) {
        @memset(&key_buf, 'a' + @as(u8, @intCast(i % 26)));
        @memset(&val_buf, 'A' + @as(u8, @intCast(i % 26)));
        leafInsertEntry(&buf, i, &key_buf, &val_buf) catch |err| {
            try testing.expectEqual(PageError.PageFull, err);
            break;
        };
        i += 1;
    }
    // Verify at least some entries were inserted
    try testing.expect(i > 0);
    // Verify all inserted entries are still intact
    for (0..i) |j| {
        const expected_char = 'a' + @as(u8, @intCast(j % 26));
        var expected_key: [10]u8 = undefined;
        @memset(&expected_key, expected_char);
        try testing.expectEqualStrings(&expected_key, leafGetKey(&buf, @intCast(j)));
    }
}

// --- Phase D: Leaf Binary Search (subtasks 9-10) ---

fn testKeyCmp(a: []const u8, b: []const u8) math.Order {
    return mem.order(u8, a, b);
}

test "subtask 9: find existing key" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);

    const keys = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee" };
    for (keys, 0..) |k, i| {
        try leafInsertEntry(&buf, @intCast(i), k, k);
    }

    // Find each key
    for (keys, 0..) |k, i| {
        const found = leafFindKey(&buf, k, testKeyCmp);
        try testing.expect(found != null);
        try testing.expectEqual(@as(u16, @intCast(i)), found.?);
        try testing.expectEqualStrings(k, leafGetValue(&buf, found.?));
    }

    // Non-existent key
    try testing.expectEqual(@as(?u16, null), leafFindKey(&buf, "zzz", testKeyCmp));
    try testing.expectEqual(@as(?u16, null), leafFindKey(&buf, "000", testKeyCmp));
}

test "subtask 10: search insertion point" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);

    try leafInsertEntry(&buf, 0, "aaa", "v1");
    try leafInsertEntry(&buf, 1, "ccc", "v2");
    try leafInsertEntry(&buf, 2, "eee", "v3");

    try testing.expectEqual(@as(u16, 0), leafSearchPoint(&buf, "000", testKeyCmp));
    try testing.expectEqual(@as(u16, 0), leafSearchPoint(&buf, "aaa", testKeyCmp));
    try testing.expectEqual(@as(u16, 1), leafSearchPoint(&buf, "bbb", testKeyCmp));
    try testing.expectEqual(@as(u16, 1), leafSearchPoint(&buf, "ccc", testKeyCmp));
    try testing.expectEqual(@as(u16, 2), leafSearchPoint(&buf, "ddd", testKeyCmp));
    try testing.expectEqual(@as(u16, 3), leafSearchPoint(&buf, "zzz", testKeyCmp));
}

// --- Phase E: Leaf Split (subtasks 11-12) ---

test "subtask 11: split distribution" {
    var buf: [256]u8 = undefined;
    var right_buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);

    // Fill with entries
    const keys = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee", "fff" };
    for (keys, 0..) |k, i| {
        try leafInsertEntry(&buf, @intCast(i), k, k);
    }
    const total = leafEntryCount(&buf);

    const result = leafSplit(&buf, &right_buf);
    const left_count = leafEntryCount(&buf);
    const right_count = leafEntryCount(&right_buf);

    // All entries present
    try testing.expectEqual(total, left_count + right_count);

    // Roughly half/half
    try testing.expect(left_count > 0);
    try testing.expect(right_count > 0);

    // Left max < right min
    const left_max = leafGetKey(&buf, left_count - 1);
    const right_min = leafGetKey(&right_buf, 0);
    try testing.expectEqual(math.Order.lt, mem.order(u8, left_max, right_min));

    // Separator is right's first key
    try testing.expectEqualStrings(right_min, result.separator_key);
}

test "subtask 12: split sibling pointers" {
    var buf: [256]u8 = undefined;
    var right_buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);
    setLeafPrevPage(&buf, 10);
    setLeafNextPage(&buf, 20);

    // Insert enough entries
    const keys = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee", "fff" };
    for (keys, 0..) |k, i| {
        try leafInsertEntry(&buf, @intCast(i), k, k);
    }

    _ = leafSplit(&buf, &right_buf);

    try testing.expectEqual(@as(u64, 10), getLeafPrevPage(&buf));
    try testing.expectEqual(@as(u64, 0), getLeafNextPage(&buf));
    try testing.expectEqual(@as(u64, 0), getLeafPrevPage(&right_buf));
    try testing.expectEqual(@as(u64, 20), getLeafNextPage(&right_buf));
}

// --- Phase F: Branch Init & Insert (subtasks 13-15) ---

test "subtask 13: branch init" {
    var buf: [256]u8 = undefined;
    initBranch(&buf, .eav, 42);
    const h = readHeader(&buf);
    try testing.expectEqual(PageType.branch, h.page_type);
    try testing.expectEqual(IndexId.eav, h.index_id);
    try testing.expectEqual(@as(u16, 0), h.num_entries);
    try testing.expectEqual(@as(u64, 42), branchGetRightChild(&buf));
}

test "subtask 14: insert first separator" {
    var buf: [256]u8 = undefined;
    initBranch(&buf, .eav, 100);
    try branchInsertEntry(&buf, 0, "mmm", 50);
    try testing.expectEqual(@as(u16, 1), branchEntryCount(&buf));
    try testing.expectEqualStrings("mmm", branchGetKey(&buf, 0));
    try testing.expectEqual(@as(u64, 50), branchGetChild(&buf, 0));
    try testing.expectEqual(@as(u64, 100), branchGetRightChild(&buf));
}

test "subtask 15: insert multiple sorted" {
    var buf: [256]u8 = undefined;
    initBranch(&buf, .eav, 100);
    try branchInsertEntry(&buf, 0, "ddd", 40);
    try branchInsertEntry(&buf, 1, "hhh", 80);
    try branchInsertEntry(&buf, 2, "mmm", 90);

    try testing.expectEqual(@as(u16, 3), branchEntryCount(&buf));
    try testing.expectEqualStrings("ddd", branchGetKey(&buf, 0));
    try testing.expectEqualStrings("hhh", branchGetKey(&buf, 1));
    try testing.expectEqualStrings("mmm", branchGetKey(&buf, 2));
    try testing.expectEqual(@as(u64, 40), branchGetChild(&buf, 0));
    try testing.expectEqual(@as(u64, 80), branchGetChild(&buf, 1));
    try testing.expectEqual(@as(u64, 90), branchGetChild(&buf, 2));
}

// --- Phase G: Branch Find Child (subtask 16) ---

test "subtask 16: findChild routing" {
    var buf: [256]u8 = undefined;
    initBranch(&buf, .eav, 100);
    try branchInsertEntry(&buf, 0, "ddd", 40);
    try branchInsertEntry(&buf, 1, "hhh", 80);

    // key < "ddd" → child 40
    try testing.expectEqual(@as(u64, 40), branchFindChild(&buf, "aaa", testKeyCmp));
    // key == "ddd" → past ddd, goes to next (80), then past hhh? No: "ddd" >= "ddd" → continue
    // "ddd" >= "ddd" → not .lt → continue. "ddd" < "hhh" → .lt → child 80
    try testing.expectEqual(@as(u64, 80), branchFindChild(&buf, "ddd", testKeyCmp));
    // key "fff" < "hhh" → child 80? No: "fff" < "ddd"? no. "fff" < "hhh"? yes → child 80
    try testing.expectEqual(@as(u64, 80), branchFindChild(&buf, "fff", testKeyCmp));
    // key "zzz" >= all → right_child 100
    try testing.expectEqual(@as(u64, 100), branchFindChild(&buf, "zzz", testKeyCmp));
}

// --- Phase H: Branch Split (subtask 17) ---

test "subtask 17: branch split" {
    var buf: [256]u8 = undefined;
    var right_buf: [256]u8 = undefined;
    initBranch(&buf, .eav, 100);

    // Insert enough separators to fill up
    try branchInsertEntry(&buf, 0, "bbb", 10);
    try branchInsertEntry(&buf, 1, "ddd", 20);
    try branchInsertEntry(&buf, 2, "fff", 30);
    try branchInsertEntry(&buf, 3, "hhh", 40);
    try branchInsertEntry(&buf, 4, "jjj", 50);

    const total = branchEntryCount(&buf);
    const mid = total / 2; // 2

    const result = branchSplit(&buf, &right_buf);
    const left_count = branchEntryCount(&buf);
    const right_count = branchEntryCount(&right_buf);

    // Middle key promoted
    try testing.expectEqualStrings("fff", result.separator_key);

    // Left has [0..mid) = 2 entries
    try testing.expectEqual(mid, left_count);
    try testing.expectEqualStrings("bbb", branchGetKey(&buf, 0));
    try testing.expectEqualStrings("ddd", branchGetKey(&buf, 1));

    // Left's right_child = mid's child
    try testing.expectEqual(@as(u64, 30), branchGetRightChild(&buf));

    // Right has [mid+1..N) = 2 entries
    try testing.expectEqual(total - mid - 1, right_count);
    try testing.expectEqualStrings("hhh", branchGetKey(&right_buf, 0));
    try testing.expectEqualStrings("jjj", branchGetKey(&right_buf, 1));

    // Right's right_child = old right_child
    try testing.expectEqual(@as(u64, 100), branchGetRightChild(&right_buf));
}

// --- Phase I: Overflow & Free Pages (subtasks 18-21) ---

test "subtask 18: overflow init and read/write" {
    var buf: [256]u8 = undefined;
    const data = "hello, this is overflow data that spans a page!";
    initOverflow(&buf, data, 0);

    try testing.expectEqual(@as(u64, 0), overflowGetNextPage(&buf));
    try testing.expectEqual(@as(u32, @intCast(data.len)), overflowGetDataLen(&buf));
    try testing.expectEqualStrings(data, overflowGetData(&buf));

    const h = readHeader(&buf);
    try testing.expectEqual(PageType.overflow, h.page_type);
}

test "subtask 19: overflow capacity" {
    try testing.expectEqual(@as(u16, 236), overflowCapacity(256));
    try testing.expectEqual(@as(u16, 4076), overflowCapacity(4096));
}

test "subtask 20: overflow chain (manual)" {
    const original = "A" ** 200 ++ "B" ** 200 ++ "C" ** 200;

    var page1: [256]u8 = undefined;
    var page2: [256]u8 = undefined;
    var page3: [256]u8 = undefined;

    const cap = overflowCapacity(256);

    // Chain: page1 -> page2 -> page3
    initOverflow(&page1, original[0..cap], 2); // next = page 2
    initOverflow(&page2, original[cap .. cap * 2], 3); // next = page 3
    const remaining = original.len - cap * 2;
    initOverflow(&page3, original[cap * 2 ..][0..remaining], 0); // last page

    // Reconstruct
    var reconstructed: [600]u8 = undefined;
    var pos: usize = 0;

    const d1 = overflowGetData(&page1);
    @memcpy(reconstructed[pos..][0..d1.len], d1);
    pos += d1.len;

    const d2 = overflowGetData(&page2);
    @memcpy(reconstructed[pos..][0..d2.len], d2);
    pos += d2.len;

    const d3 = overflowGetData(&page3);
    @memcpy(reconstructed[pos..][0..d3.len], d3);
    pos += d3.len;

    try testing.expectEqualStrings(original, reconstructed[0..pos]);

    // Verify chain links
    try testing.expectEqual(@as(u64, 2), overflowGetNextPage(&page1));
    try testing.expectEqual(@as(u64, 3), overflowGetNextPage(&page2));
    try testing.expectEqual(@as(u64, 0), overflowGetNextPage(&page3));
}

test "subtask 21: free page init" {
    var buf: [256]u8 = undefined;
    initFree(&buf);
    const h = readHeader(&buf);
    try testing.expectEqual(PageType.free, h.page_type);
    try testing.expectEqual(IndexId.free, h.index_id);
    try testing.expectEqual(@as(u16, 0), h.num_entries);
}

// --- Phase J: Integration Tests (subtasks 22-24) ---

test "subtask 22: encoded EAV keys in leaf" {
    var buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);

    // Create encoded keys
    var key1: [64]u8 = undefined;
    var key2: [64]u8 = undefined;
    var key3: [64]u8 = undefined;
    const n1 = encoding.encode(.{ .i64 = 10 }, &key1);
    const n2 = encoding.encode(.{ .i64 = 20 }, &key2);
    const n3 = encoding.encode(.{ .i64 = 30 }, &key3);

    try leafInsertEntry(&buf, 0, key1[0..n1], "val10");
    try leafInsertEntry(&buf, 1, key2[0..n2], "val20");
    try leafInsertEntry(&buf, 2, key3[0..n3], "val30");

    const cmp_fn: KeyCompareFn = struct {
        fn cmp(a: []const u8, b: []const u8) math.Order {
            return encoding.compareEncoded(a, b);
        }
    }.cmp;

    // Lookup key 20
    const found = leafFindKey(&buf, key2[0..n2], cmp_fn);
    try testing.expect(found != null);
    try testing.expectEqualStrings("val20", leafGetValue(&buf, found.?));

    // Key not present
    var key_missing: [64]u8 = undefined;
    const nm = encoding.encode(.{ .i64 = 15 }, &key_missing);
    try testing.expectEqual(@as(?u16, null), leafFindKey(&buf, key_missing[0..nm], cmp_fn));
}

test "subtask 23: branch+leaf routing simulation" {
    // Simulate: branch with separator "mmm" routing to two leaves
    var branch_buf: [256]u8 = undefined;
    var leaf_left: [256]u8 = undefined;
    var leaf_right: [256]u8 = undefined;

    initLeaf(&leaf_left, .eav);
    initLeaf(&leaf_right, .eav);
    initBranch(&branch_buf, .eav, 2); // right_child = page 2

    try leafInsertEntry(&leaf_left, 0, "aaa", "v1");
    try leafInsertEntry(&leaf_left, 1, "fff", "v2");
    try leafInsertEntry(&leaf_right, 0, "mmm", "v3");
    try leafInsertEntry(&leaf_right, 1, "zzz", "v4");

    try branchInsertEntry(&branch_buf, 0, "mmm", 1); // child=1 for keys < "mmm"

    // Route "aaa" → should go to child 1 (left leaf, page 1)
    const child_aaa = branchFindChild(&branch_buf, "aaa", testKeyCmp);
    try testing.expectEqual(@as(u64, 1), child_aaa);

    // In the left leaf, find "aaa"
    const found_aaa = leafFindKey(&leaf_left, "aaa", testKeyCmp);
    try testing.expect(found_aaa != null);
    try testing.expectEqualStrings("v1", leafGetValue(&leaf_left, found_aaa.?));

    // Route "zzz" → should go to right_child (page 2)
    const child_zzz = branchFindChild(&branch_buf, "zzz", testKeyCmp);
    try testing.expectEqual(@as(u64, 2), child_zzz);

    // In the right leaf, find "zzz"
    const found_zzz = leafFindKey(&leaf_right, "zzz", testKeyCmp);
    try testing.expect(found_zzz != null);
    try testing.expectEqualStrings("v4", leafGetValue(&leaf_right, found_zzz.?));
}

test "subtask 24: split then re-lookup all" {
    var buf: [256]u8 = undefined;
    var right_buf: [256]u8 = undefined;
    initLeaf(&buf, .eav);

    const keys = [_][]const u8{ "aaa", "bbb", "ccc", "ddd", "eee", "fff" };
    for (keys, 0..) |k, i| {
        try leafInsertEntry(&buf, @intCast(i), k, k);
    }

    _ = leafSplit(&buf, &right_buf);

    // Every original key must be findable in exactly one page
    for (keys) |k| {
        const in_left = leafFindKey(&buf, k, testKeyCmp);
        const in_right = leafFindKey(&right_buf, k, testKeyCmp);

        // Exactly one should be non-null
        const found_left = in_left != null;
        const found_right = in_right != null;
        try testing.expect(found_left != found_right); // XOR

        // Verify value matches
        if (in_left) |idx| {
            try testing.expectEqualStrings(k, leafGetValue(&buf, idx));
        }
        if (in_right) |idx| {
            try testing.expectEqualStrings(k, leafGetValue(&right_buf, idx));
        }
    }
}
