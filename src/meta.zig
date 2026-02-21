// ZatDB — Meta page format with dual-page crash recovery
//
// Two meta pages (slots 0 and 1) occupy the first two pages of the .zat file.
// On commit, the older slot is overwritten. On recovery, the valid page with
// the higher tx_id wins. If only one is valid, it's used. If both are corrupt,
// the database cannot be opened.
//
// Meta header layout (92 bytes):
//   [magic: 4][version: 4][page_size: 4][flags: 4]
//   [tx_id: 8][eav_root: 8][ave_root: 8][vae_root: 8][txlog_root: 8][free_root: 8]
//   [next_entity_id: 8][next_page: 8][datom_count: 8]
//   [checksum: 4]

const std = @import("std");
const encoding = @import("encoding.zig");
const testing = std.testing;

pub const MAGIC: u32 = 0x5A415444;
pub const VERSION: u32 = 1;
pub const META_HEADER_SIZE: usize = 92;

pub const MetaPage = struct {
    magic: u32 = MAGIC,
    version: u32 = VERSION,
    page_size: u32 = 0,
    flags: u32 = 0,
    tx_id: u64 = 0,
    eav_root_page: u64 = 0,
    ave_root_page: u64 = 0,
    vae_root_page: u64 = 0,
    txlog_root_page: u64 = 0,
    free_root_page: u64 = 0,
    next_entity_id: u64 = 0,
    next_page: u64 = 0,
    datom_count: u64 = 0,
    checksum: u32 = 0,
};

/// Compute CRC-32 checksum over the first 88 bytes of a meta page header.
pub fn computeChecksum(bytes: *const [88]u8) u32 {
    return std.hash.Crc32.hash(bytes);
}

/// Serialize a MetaPage to a byte buffer. Auto-computes and writes the checksum.
/// Buffer must be at least META_HEADER_SIZE (92) bytes.
pub fn writeMeta(buf: []u8, m: MetaPage) void {
    std.debug.assert(buf.len >= META_HEADER_SIZE);
    encoding.writeU32(buf[0..4], m.magic);
    encoding.writeU32(buf[4..8], m.version);
    encoding.writeU32(buf[8..12], m.page_size);
    encoding.writeU32(buf[12..16], m.flags);
    encoding.writeU64(buf[16..24], m.tx_id);
    encoding.writeU64(buf[24..32], m.eav_root_page);
    encoding.writeU64(buf[32..40], m.ave_root_page);
    encoding.writeU64(buf[40..48], m.vae_root_page);
    encoding.writeU64(buf[48..56], m.txlog_root_page);
    encoding.writeU64(buf[56..64], m.free_root_page);
    encoding.writeU64(buf[64..72], m.next_entity_id);
    encoding.writeU64(buf[72..80], m.next_page);
    encoding.writeU64(buf[80..88], m.datom_count);
    const checksum = computeChecksum(buf[0..88]);
    encoding.writeU32(buf[88..92], checksum);
}

/// Deserialize a MetaPage from a byte buffer (at least META_HEADER_SIZE bytes).
pub fn readMeta(buf: []const u8) MetaPage {
    return .{
        .magic = encoding.readU32(buf[0..4]),
        .version = encoding.readU32(buf[4..8]),
        .page_size = encoding.readU32(buf[8..12]),
        .flags = encoding.readU32(buf[12..16]),
        .tx_id = encoding.readU64(buf[16..24]),
        .eav_root_page = encoding.readU64(buf[24..32]),
        .ave_root_page = encoding.readU64(buf[32..40]),
        .vae_root_page = encoding.readU64(buf[40..48]),
        .txlog_root_page = encoding.readU64(buf[48..56]),
        .free_root_page = encoding.readU64(buf[56..64]),
        .next_entity_id = encoding.readU64(buf[64..72]),
        .next_page = encoding.readU64(buf[72..80]),
        .datom_count = encoding.readU64(buf[80..88]),
        .checksum = encoding.readU32(buf[88..92]),
    };
}

/// Validate a raw meta page buffer. Checks magic, version, and CRC-32 checksum.
pub fn isValidMeta(buf: []const u8) bool {
    if (buf.len < META_HEADER_SIZE) return false;
    const magic = encoding.readU32(buf[0..4]);
    if (magic != MAGIC) return false;
    const version = encoding.readU32(buf[4..8]);
    if (version != VERSION) return false;
    const stored = encoding.readU32(buf[88..92]);
    const computed = computeChecksum(buf[0..88]);
    return stored == computed;
}

/// Pick the active meta from two candidate buffers (dual-page recovery).
/// Returns the valid page with the higher tx_id, or null if both are corrupt.
pub fn pickActiveMeta(buf0: []const u8, buf1: []const u8) ?MetaPage {
    const valid0 = isValidMeta(buf0);
    const valid1 = isValidMeta(buf1);
    if (valid0 and valid1) {
        const m0 = readMeta(buf0);
        const m1 = readMeta(buf1);
        return if (m0.tx_id >= m1.tx_id) m0 else m1;
    } else if (valid0) {
        return readMeta(buf0);
    } else if (valid1) {
        return readMeta(buf1);
    } else {
        return null;
    }
}

// ============================================================================
// Tests
// ============================================================================

test "subtask 1: meta constants" {
    try testing.expectEqual(@as(u32, 0x5A415444), MAGIC);
    try testing.expectEqual(@as(u32, 1), VERSION);
    try testing.expectEqual(@as(usize, 92), META_HEADER_SIZE);
}

test "subtask 2: meta write/read round-trip" {
    var buf: [256]u8 = undefined;
    @memset(&buf, 0);

    const original = MetaPage{
        .page_size = 4096,
        .tx_id = 42,
        .eav_root_page = 10,
        .ave_root_page = 20,
        .vae_root_page = 30,
        .txlog_root_page = 40,
        .free_root_page = 50,
        .next_entity_id = 1000,
        .next_page = 100,
        .datom_count = 500,
    };
    writeMeta(&buf, original);
    const m = readMeta(&buf);

    try testing.expectEqual(MAGIC, m.magic);
    try testing.expectEqual(VERSION, m.version);
    try testing.expectEqual(@as(u32, 4096), m.page_size);
    try testing.expectEqual(@as(u32, 0), m.flags);
    try testing.expectEqual(@as(u64, 42), m.tx_id);
    try testing.expectEqual(@as(u64, 10), m.eav_root_page);
    try testing.expectEqual(@as(u64, 20), m.ave_root_page);
    try testing.expectEqual(@as(u64, 30), m.vae_root_page);
    try testing.expectEqual(@as(u64, 40), m.txlog_root_page);
    try testing.expectEqual(@as(u64, 50), m.free_root_page);
    try testing.expectEqual(@as(u64, 1000), m.next_entity_id);
    try testing.expectEqual(@as(u64, 100), m.next_page);
    try testing.expectEqual(@as(u64, 500), m.datom_count);
}

test "subtask 3: checksum non-zero, deterministic; corruption changes it" {
    var buf: [256]u8 = undefined;
    @memset(&buf, 0);
    writeMeta(&buf, .{ .page_size = 4096, .tx_id = 1 });

    const checksum = encoding.readU32(buf[88..92]);
    try testing.expect(checksum != 0);

    // Same input → same checksum
    var buf2: [256]u8 = undefined;
    @memset(&buf2, 0);
    writeMeta(&buf2, .{ .page_size = 4096, .tx_id = 1 });
    try testing.expectEqual(checksum, encoding.readU32(buf2[88..92]));

    // Corrupt one byte → different checksum
    buf2[20] ^= 0xFF;
    const corrupted = computeChecksum(buf2[0..88]);
    try testing.expect(corrupted != checksum);
}

test "subtask 4: isValidMeta" {
    var buf: [256]u8 = undefined;
    @memset(&buf, 0);
    writeMeta(&buf, .{ .page_size = 4096, .tx_id = 1 });

    try testing.expect(isValidMeta(&buf));

    // Wrong magic
    var bad_magic = buf;
    bad_magic[0] = 0xFF;
    try testing.expect(!isValidMeta(&bad_magic));

    // Wrong version
    var bad_version = buf;
    encoding.writeU32(bad_version[4..8], 99);
    try testing.expect(!isValidMeta(&bad_version));

    // Wrong checksum
    var bad_checksum = buf;
    bad_checksum[88] ^= 0xFF;
    try testing.expect(!isValidMeta(&bad_checksum));

    // Buffer too small
    try testing.expect(!isValidMeta(buf[0..10]));

    // All zeros (invalid magic)
    var zeros: [256]u8 = undefined;
    @memset(&zeros, 0);
    try testing.expect(!isValidMeta(&zeros));
}

test "subtask 5: pickActiveMeta recovery" {
    var buf0: [256]u8 = undefined;
    var buf1: [256]u8 = undefined;
    @memset(&buf0, 0);
    @memset(&buf1, 0);

    // Both valid → pick higher tx_id
    writeMeta(&buf0, .{ .page_size = 4096, .tx_id = 5 });
    writeMeta(&buf1, .{ .page_size = 4096, .tx_id = 10 });
    {
        const active = pickActiveMeta(&buf0, &buf1).?;
        try testing.expectEqual(@as(u64, 10), active.tx_id);
    }

    // Reversed: buf0 higher
    writeMeta(&buf0, .{ .page_size = 4096, .tx_id = 20 });
    writeMeta(&buf1, .{ .page_size = 4096, .tx_id = 10 });
    {
        const active = pickActiveMeta(&buf0, &buf1).?;
        try testing.expectEqual(@as(u64, 20), active.tx_id);
    }

    // One corrupt → pick the other
    var corrupt: [256]u8 = undefined;
    @memset(&corrupt, 0xFF);
    {
        const active = pickActiveMeta(&corrupt, &buf1).?;
        try testing.expectEqual(@as(u64, 10), active.tx_id);
    }
    {
        const active = pickActiveMeta(&buf0, &corrupt).?;
        try testing.expectEqual(@as(u64, 20), active.tx_id);
    }

    // Both corrupt → null
    var corrupt2: [256]u8 = undefined;
    @memset(&corrupt2, 0);
    try testing.expectEqual(@as(?MetaPage, null), pickActiveMeta(&corrupt, &corrupt2));
}
