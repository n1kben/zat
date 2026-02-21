// ZatDB — Value encoding/decoding with sortable binary format
//
// All values are serialized as a type-tag byte followed by type-specific payload.
// The encoding preserves sort order: compareEncoded(encode(a), encode(b)) == compare(a, b).
//
// Tag  Type       Payload
// 0x00 nil        (0 bytes)
// 0x01 boolean    1 byte (0x00 = false, 0x01 = true)
// 0x02 i64        8 bytes, big-endian with sign-flip
// 0x03 f64        8 bytes, IEEE 754 with sign-flip
// 0x04 string     4-byte length (BE) + UTF-8 bytes
// 0x05 keyword    4-byte length (BE) + UTF-8 bytes
// 0x06 ref        8 bytes, big-endian u64
// 0x07 instant    8 bytes, big-endian i64 (epoch microseconds) with sign-flip
// 0x08 uuid       16 bytes, big-endian
// 0x09 bytes      4-byte length (BE) + raw bytes

const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;

/// Type tags for encoded values. Numeric order defines cross-type sort order.
pub const Tag = enum(u8) {
    nil = 0x00,
    boolean = 0x01,
    i64 = 0x02,
    f64 = 0x03,
    string = 0x04,
    keyword = 0x05,
    ref = 0x06,
    instant = 0x07,
    uuid = 0x08,
    bytes = 0x09,
};

/// A decoded value.
pub const Value = union(Tag) {
    nil: void,
    boolean: bool,
    i64: i64,
    f64: f64,
    string: []const u8,
    keyword: []const u8,
    ref: u64,
    instant: i64,
    uuid: [16]u8,
    bytes: []const u8,

    /// Compare two values of the same type. Cross-type comparison uses tag order.
    pub fn order(a: Value, b: Value) math.Order {
        const tag_a: u8 = @intFromEnum(std.meta.activeTag(a));
        const tag_b: u8 = @intFromEnum(std.meta.activeTag(b));
        if (tag_a != tag_b) return math.order(tag_a, tag_b);

        return switch (a) {
            .nil => .eq,
            .boolean => |va| {
                const ia: u8 = @intFromBool(va);
                const ib: u8 = @intFromBool(b.boolean);
                return math.order(ia, ib);
            },
            .i64 => |va| math.order(va, b.i64),
            .f64 => |va| orderF64(va, b.f64),
            .string => |va| mem.order(u8, va, b.string),
            .keyword => |va| mem.order(u8, va, b.keyword),
            .ref => |va| math.order(va, b.ref),
            .instant => |va| math.order(va, b.instant),
            .uuid => |va| mem.order(u8, &va, &b.uuid),
            .bytes => |va| mem.order(u8, va, b.bytes),
        };
    }

    pub fn eql(a: Value, b: Value) bool {
        return a.order(b) == .eq;
    }
};

/// Maximum encoded size for a value (tag + max payload).
/// Variable-length types (string, keyword, bytes) need tag(1) + len(4) + data.
pub fn encodedSize(v: Value) usize {
    return switch (v) {
        .nil => 1,
        .boolean => 2,
        .i64 => 9,
        .f64 => 9,
        .string => |s| 1 + 4 + s.len,
        .keyword => |k| 1 + 4 + k.len,
        .ref => 9,
        .instant => 9,
        .uuid => 17,
        .bytes => |b| 1 + 4 + b.len,
    };
}

/// Encode a value into `buf`. Returns number of bytes written.
/// Caller must ensure buf is large enough (use `encodedSize`).
pub fn encode(v: Value, buf: []u8) usize {
    buf[0] = @intFromEnum(std.meta.activeTag(v));
    switch (v) {
        .nil => return 1,
        .boolean => |b| {
            buf[1] = @intFromBool(b);
            return 2;
        },
        .i64 => |val| {
            writeU64(buf[1..9], encodeSortableI64(val));
            return 9;
        },
        .f64 => |val| {
            writeU64(buf[1..9], encodeSortableF64(val));
            return 9;
        },
        .string => |s| return encodeVarLen(buf, s),
        .keyword => |k| return encodeVarLen(buf, k),
        .ref => |r| {
            writeU64(buf[1..9], r);
            return 9;
        },
        .instant => |val| {
            writeU64(buf[1..9], encodeSortableI64(val));
            return 9;
        },
        .uuid => |u| {
            @memcpy(buf[1..17], &u);
            return 17;
        },
        .bytes => |b| return encodeVarLen(buf, b),
    }
}

/// Decode a value from bytes. Variable-length types reference the input slice (zero-copy).
pub fn decode(data: []const u8) Value {
    const tag: Tag = @enumFromInt(data[0]);
    return switch (tag) {
        .nil => .{ .nil = {} },
        .boolean => .{ .boolean = data[1] != 0 },
        .i64 => .{ .i64 = decodeSortableI64(readU64(data[1..9])) },
        .f64 => .{ .f64 = decodeSortableF64(readU64(data[1..9])) },
        .string => .{ .string = decodeVarLen(data) },
        .keyword => .{ .keyword = decodeVarLen(data) },
        .ref => .{ .ref = readU64(data[1..9]) },
        .instant => .{ .instant = decodeSortableI64(readU64(data[1..9])) },
        .uuid => .{ .uuid = data[1..17].* },
        .bytes => .{ .bytes = decodeVarLen(data) },
    };
}

/// Returns the total encoded length by examining the tag and payload.
pub fn encodedLen(data: []const u8) usize {
    const tag: Tag = @enumFromInt(data[0]);
    return switch (tag) {
        .nil => 1,
        .boolean => 2,
        .i64, .f64, .ref, .instant => 9,
        .uuid => 17,
        .string, .keyword, .bytes => 1 + 4 + readU32(data[1..5]),
    };
}

/// Bytewise comparison of two encoded values. Preserves semantic sort order.
pub fn compareEncoded(a: []const u8, b: []const u8) math.Order {
    // Tag comparison first (handles cross-type ordering)
    const tag_cmp = math.order(a[0], b[0]);
    if (tag_cmp != .eq) return tag_cmp;

    const tag: Tag = @enumFromInt(a[0]);
    return switch (tag) {
        .nil => .eq,
        // Fixed-size types: direct bytewise comparison of payload
        .boolean, .i64, .f64, .ref, .instant => {
            const size: usize = switch (tag) {
                .boolean => 2,
                .uuid => 17,
                else => 9,
            };
            return mem.order(u8, a[1..size], b[1..size]);
        },
        .uuid => mem.order(u8, a[1..17], b[1..17]),
        // Variable-length: compare data bytes (NOT length prefix)
        .string, .keyword, .bytes => {
            const a_len = readU32(a[1..5]);
            const b_len = readU32(b[1..5]);
            const a_data = a[5..][0..a_len];
            const b_data = b[5..][0..b_len];
            return mem.order(u8, a_data, b_data);
        },
    };
}

// --- Sortable encoding helpers ---

/// Encode i64 for unsigned bytewise comparison: flip sign bit.
pub fn encodeSortableI64(val: i64) u64 {
    const bits: u64 = @bitCast(val);
    return bits ^ (@as(u64, 1) << 63);
}

/// Decode sortable i64 back to i64.
pub fn decodeSortableI64(bits: u64) i64 {
    return @bitCast(bits ^ (@as(u64, 1) << 63));
}

/// Encode f64 for unsigned bytewise comparison.
/// Positive: flip sign bit. Negative: flip all bits.
/// This maps the IEEE 754 total order to unsigned integer order.
pub fn encodeSortableF64(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    if (bits >> 63 == 1) {
        // Negative: flip all bits
        return ~bits;
    } else {
        // Positive (including +0): flip sign bit
        return bits ^ (@as(u64, 1) << 63);
    }
}

/// Decode sortable f64 back to f64.
pub fn decodeSortableF64(bits: u64) f64 {
    if (bits >> 63 == 1) {
        // Was positive: flip sign bit back
        return @bitCast(bits ^ (@as(u64, 1) << 63));
    } else {
        // Was negative: flip all bits back
        return @bitCast(~bits);
    }
}

// --- Internal helpers ---

fn encodeVarLen(buf: []u8, data: []const u8) usize {
    // tag already written at buf[0]
    const len: u32 = @intCast(data.len);
    writeU32(buf[1..5], len);
    @memcpy(buf[5..][0..data.len], data);
    return 1 + 4 + data.len;
}

fn decodeVarLen(data: []const u8) []const u8 {
    const len = readU32(data[1..5]);
    return data[5..][0..len];
}

pub fn writeU16(buf: *[2]u8, val: u16) void {
    buf.* = @bitCast(@as(u16, @byteSwap(val)));
}

pub fn readU16(buf: *const [2]u8) u16 {
    return @byteSwap(@as(u16, @bitCast(buf.*)));
}

pub fn writeU64(buf: *[8]u8, val: u64) void {
    buf.* = @bitCast(@as(u64, @byteSwap(val)));
}

pub fn readU64(buf: *const [8]u8) u64 {
    return @byteSwap(@as(u64, @bitCast(buf.*)));
}

pub fn writeU32(buf: *[4]u8, val: u32) void {
    buf.* = @bitCast(@as(u32, @byteSwap(val)));
}

pub fn readU32(buf: *const [4]u8) u32 {
    return @byteSwap(@as(u32, @bitCast(buf.*)));
}

/// Compare two f64 values with a total order (NaN sorts last).
fn orderF64(a: f64, b: f64) math.Order {
    // Use sortable encoding to get a consistent total order
    return math.order(encodeSortableF64(a), encodeSortableF64(b));
}

// ============================================================================
// Tests
// ============================================================================

test "encode/decode roundtrip — nil" {
    var buf: [1]u8 = undefined;
    const n = encode(.{ .nil = {} }, &buf);
    try testing.expectEqual(1, n);
    const v = decode(&buf);
    try testing.expectEqual(Tag.nil, std.meta.activeTag(v));
}

test "encode/decode roundtrip — boolean" {
    var buf: [2]u8 = undefined;
    _ = encode(.{ .boolean = true }, &buf);
    try testing.expectEqual(true, decode(&buf).boolean);

    _ = encode(.{ .boolean = false }, &buf);
    try testing.expectEqual(false, decode(&buf).boolean);
}

test "encode/decode roundtrip — i64" {
    var buf: [9]u8 = undefined;
    const cases = [_]i64{ 0, 1, -1, 42, -42, math.maxInt(i64), math.minInt(i64), 1000000, -1000000 };
    for (cases) |val| {
        _ = encode(.{ .i64 = val }, &buf);
        try testing.expectEqual(val, decode(&buf).i64);
    }
}

test "encode/decode roundtrip — f64" {
    var buf: [9]u8 = undefined;
    const cases = [_]f64{ 0.0, -0.0, 1.0, -1.0, 3.14159, -3.14159, math.floatMax(f64), math.floatMin(f64), math.inf(f64), -math.inf(f64), math.nan(f64) };
    for (cases) |val| {
        _ = encode(.{ .f64 = val }, &buf);
        const decoded = decode(&buf).f64;
        if (math.isNan(val)) {
            try testing.expect(math.isNan(decoded));
        } else {
            try testing.expectEqual(val, decoded);
        }
    }
}

test "encode/decode roundtrip — string" {
    var buf: [256]u8 = undefined;
    const cases = [_][]const u8{ "", "hello", "ZatDB", "Unicode: \xc3\xa9\xc3\xa0\xc3\xbc" };
    for (cases) |s| {
        const n = encode(.{ .string = s }, &buf);
        try testing.expectEqual(1 + 4 + s.len, n);
        try testing.expectEqualStrings(s, decode(buf[0..n]).string);
    }
}

test "encode/decode roundtrip — keyword" {
    var buf: [256]u8 = undefined;
    const cases = [_][]const u8{ ":db/ident", ":user/name", ":db.type/string" };
    for (cases) |k| {
        const n = encode(.{ .keyword = k }, &buf);
        try testing.expectEqualStrings(k, decode(buf[0..n]).keyword);
    }
}

test "encode/decode roundtrip — ref" {
    var buf: [9]u8 = undefined;
    const cases = [_]u64{ 0, 1, 42, 0xFFFF_FFFF_FFFF_FFFF, 1 << 54 | 1 };
    for (cases) |r| {
        _ = encode(.{ .ref = r }, &buf);
        try testing.expectEqual(r, decode(&buf).ref);
    }
}

test "encode/decode roundtrip — instant" {
    var buf: [9]u8 = undefined;
    const cases = [_]i64{ 0, 1_000_000, -1_000_000, math.maxInt(i64), math.minInt(i64) };
    for (cases) |val| {
        _ = encode(.{ .instant = val }, &buf);
        try testing.expectEqual(val, decode(&buf).instant);
    }
}

test "encode/decode roundtrip — uuid" {
    var buf: [17]u8 = undefined;
    const id = [16]u8{ 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10 };
    _ = encode(.{ .uuid = id }, &buf);
    try testing.expectEqualSlices(u8, &id, &decode(&buf).uuid);
}

test "encode/decode roundtrip — bytes" {
    var buf: [256]u8 = undefined;
    const data = [_]u8{ 0x00, 0xFF, 0x42, 0x00, 0xDE, 0xAD };
    const n = encode(.{ .bytes = &data }, &buf);
    try testing.expectEqualSlices(u8, &data, decode(buf[0..n]).bytes);

    // Empty bytes
    const n2 = encode(.{ .bytes = "" }, &buf);
    try testing.expectEqual(5, n2);
    try testing.expectEqualSlices(u8, &[_]u8{}, decode(buf[0..n2]).bytes);
}

test "sortable i64 encoding preserves order" {
    const cases = [_]i64{ math.minInt(i64), -1_000_000, -1, 0, 1, 1_000_000, math.maxInt(i64) };
    for (0..cases.len - 1) |i| {
        const a = encodeSortableI64(cases[i]);
        const b = encodeSortableI64(cases[i + 1]);
        try testing.expect(a < b);
    }
}

test "sortable f64 encoding preserves order" {
    const cases = [_]f64{ -math.inf(f64), -1e100, -1.0, -math.floatMin(f64), -0.0, 0.0, math.floatMin(f64), 1.0, 1e100, math.inf(f64), math.nan(f64) };
    for (0..cases.len - 1) |i| {
        const a = encodeSortableF64(cases[i]);
        const b = encodeSortableF64(cases[i + 1]);
        try testing.expect(a < b);
    }
}

test "sortable encoding preserves order — encoded i64 values" {
    var buf_a: [9]u8 = undefined;
    var buf_b: [9]u8 = undefined;
    const cases = [_]i64{ math.minInt(i64), -1, 0, 1, math.maxInt(i64) };
    for (0..cases.len - 1) |i| {
        const na = encode(.{ .i64 = cases[i] }, &buf_a);
        const nb = encode(.{ .i64 = cases[i + 1] }, &buf_b);
        try testing.expectEqual(math.Order.lt, compareEncoded(buf_a[0..na], buf_b[0..nb]));
    }
}

test "sortable encoding preserves order — encoded f64 values" {
    var buf_a: [9]u8 = undefined;
    var buf_b: [9]u8 = undefined;
    const cases = [_]f64{ -math.inf(f64), -1.0, -0.0, 0.0, 1.0, math.inf(f64), math.nan(f64) };
    for (0..cases.len - 1) |i| {
        const na = encode(.{ .f64 = cases[i] }, &buf_a);
        const nb = encode(.{ .f64 = cases[i + 1] }, &buf_b);
        try testing.expectEqual(math.Order.lt, compareEncoded(buf_a[0..na], buf_b[0..nb]));
    }
}

test "sortable encoding preserves order — encoded string values" {
    var buf_a: [64]u8 = undefined;
    var buf_b: [64]u8 = undefined;
    const cases = [_][]const u8{ "", "a", "aa", "ab", "b", "ba" };
    for (0..cases.len - 1) |i| {
        const na = encode(.{ .string = cases[i] }, &buf_a);
        const nb = encode(.{ .string = cases[i + 1] }, &buf_b);
        try testing.expectEqual(math.Order.lt, compareEncoded(buf_a[0..na], buf_b[0..nb]));
    }
}

test "cross-type tag ordering" {
    var buf_nil: [1]u8 = undefined;
    var buf_bool: [2]u8 = undefined;
    var buf_i64: [9]u8 = undefined;
    var buf_str: [10]u8 = undefined;

    const n_nil = encode(.{ .nil = {} }, &buf_nil);
    const n_bool = encode(.{ .boolean = false }, &buf_bool);
    const n_i64 = encode(.{ .i64 = 0 }, &buf_i64);
    const n_str = encode(.{ .string = "a" }, &buf_str);

    // nil < bool < i64 < ... < string
    try testing.expectEqual(math.Order.lt, compareEncoded(buf_nil[0..n_nil], buf_bool[0..n_bool]));
    try testing.expectEqual(math.Order.lt, compareEncoded(buf_bool[0..n_bool], buf_i64[0..n_i64]));
    try testing.expectEqual(math.Order.lt, compareEncoded(buf_i64[0..n_i64], buf_str[0..n_str]));
}

test "Value.order matches compareEncoded" {
    var buf_a: [64]u8 = undefined;
    var buf_b: [64]u8 = undefined;

    const pairs = [_][2]Value{
        .{ .{ .i64 = -5 }, .{ .i64 = 5 } },
        .{ .{ .i64 = 0 }, .{ .i64 = 0 } },
        .{ .{ .f64 = -1.0 }, .{ .f64 = 1.0 } },
        .{ .{ .string = "abc" }, .{ .string = "abd" } },
        .{ .{ .boolean = false }, .{ .boolean = true } },
        .{ .{ .nil = {} }, .{ .boolean = false } },
        .{ .{ .ref = 1 }, .{ .ref = 2 } },
    };

    for (pairs) |pair| {
        const na = encode(pair[0], &buf_a);
        const nb = encode(pair[1], &buf_b);
        try testing.expectEqual(pair[0].order(pair[1]), compareEncoded(buf_a[0..na], buf_b[0..nb]));
    }
}

test "encodedSize matches actual encoded length" {
    var buf: [256]u8 = undefined;
    const values = [_]Value{
        .{ .nil = {} },
        .{ .boolean = true },
        .{ .i64 = 42 },
        .{ .f64 = 3.14 },
        .{ .string = "hello" },
        .{ .keyword = ":db/ident" },
        .{ .ref = 100 },
        .{ .instant = 1000 },
        .{ .uuid = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 } },
        .{ .bytes = "raw" },
    };
    for (values) |v| {
        const n = encode(v, &buf);
        try testing.expectEqual(encodedSize(v), n);
        try testing.expectEqual(n, encodedLen(buf[0..n]));
    }
}

test "encodeSortableI64 roundtrip" {
    const cases = [_]i64{ math.minInt(i64), -1, 0, 1, math.maxInt(i64) };
    for (cases) |val| {
        try testing.expectEqual(val, decodeSortableI64(encodeSortableI64(val)));
    }
}

test "encodeSortableF64 roundtrip" {
    const cases = [_]f64{ -math.inf(f64), -1.0, -0.0, 0.0, 1.0, math.inf(f64) };
    for (cases) |val| {
        try testing.expectEqual(val, decodeSortableF64(encodeSortableF64(val)));
    }
    // NaN roundtrip (NaN != NaN, but bits should match)
    const nan_bits = encodeSortableF64(math.nan(f64));
    try testing.expect(math.isNan(decodeSortableF64(nan_bits)));
}
