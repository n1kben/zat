// ZatDB — Schema layer: EAV encoding, entity IDs, bootstrap & schema cache
//
// Bridges raw B+ tree storage and Datomic-style entity-attribute-value semantics.
// On first open, bootstraps meta-schema attributes (IDs 1-8). On subsequent
// opens, loads schema from the EAV index into an in-memory cache.
//
// EAV key encoding:
//   [entity_id: 8 BE][attr_id: 8 BE][encoded_value: variable]
//   Value portion is empty — key comparison is plain memcmp.

const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;
const encoding = @import("encoding.zig");
const btree = @import("btree.zig");
const file = @import("file.zig");
const page = @import("page.zig");

// ============================================================================
// Constants
// ============================================================================

/// Well-known schema attribute entity IDs (partition 0, :db.part/db).
pub const ATTR_IDENT = 1;
pub const ATTR_VALUE_TYPE = 2;
pub const ATTR_CARDINALITY = 3;
pub const ATTR_UNIQUE = 4;
pub const ATTR_INDEX = 5;
pub const ATTR_IS_COMPONENT = 6;
pub const ATTR_DOC = 7;
pub const ATTR_TX_INSTANT = 8;

/// Partition tags (bits 63..54 of entity ID).
pub const PARTITION_DB = 0;
pub const PARTITION_TX = 1;
pub const PARTITION_USER = 2;

/// Maximum length for a keyword ident string.
pub const MAX_IDENT_LEN = 128;

/// Maximum number of schema attributes in the cache.
pub const MAX_ATTRS = 256;

/// Size of the fixed E+A prefix in an EAV key.
const EAV_PREFIX_SIZE = 16; // 8 bytes entity + 8 bytes attr

// ============================================================================
// Entity ID helpers
// ============================================================================

/// Construct an entity ID from a partition tag and sequence number.
/// Bits 63..54 = partition (10 bits), bits 53..0 = sequence (54 bits).
pub fn makeEntityId(partition: u10, seq: u54) u64 {
    return (@as(u64, partition) << 54) | @as(u64, seq);
}

/// Extract the partition tag from an entity ID.
pub fn partitionOf(eid: u64) u10 {
    return @intCast(eid >> 54);
}

// ============================================================================
// Enums
// ============================================================================

pub const Cardinality = enum { one, many };

pub const Uniqueness = enum { none, value, identity };

// ============================================================================
// EAV key encoding
// ============================================================================

/// Encode an EAV key: [entity_id: 8 BE][attr_id: 8 BE][encoded_value].
/// Returns the total key length written into buf.
pub fn encodeEavKey(buf: []u8, entity: u64, attr: u64, value: encoding.Value) usize {
    encoding.writeU64(buf[0..8], entity);
    encoding.writeU64(buf[8..16], attr);
    const val_len = encoding.encode(value, buf[16..]);
    return EAV_PREFIX_SIZE + val_len;
}

/// Decode the entity ID from an EAV key.
pub fn decodeEavEntity(key: []const u8) u64 {
    return encoding.readU64(key[0..8]);
}

/// Decode the attribute ID from an EAV key.
pub fn decodeEavAttr(key: []const u8) u64 {
    return encoding.readU64(key[8..16]);
}

/// Decode the value from an EAV key (bytes after the E+A prefix).
pub fn decodeEavValue(key: []const u8) encoding.Value {
    return encoding.decode(key[16..]);
}

/// EAV key comparison — plain memcmp. Entity sorts first (BE), then attribute, then value.
pub fn eavKeyCmp(a: []const u8, b: []const u8) math.Order {
    return mem.order(u8, a, b);
}

// ============================================================================
// SchemaAttr
// ============================================================================

pub const SchemaAttr = struct {
    eid: u64,
    ident_buf: [MAX_IDENT_LEN]u8 = .{0} ** MAX_IDENT_LEN,
    ident_len: u8 = 0,
    value_type: encoding.Tag = .nil,
    cardinality: Cardinality = .one,
    unique: Uniqueness = .none,
    indexed: bool = false,
    is_component: bool = false,

    /// Get the ident keyword string.
    pub fn ident(self: *const SchemaAttr) []const u8 {
        return self.ident_buf[0..self.ident_len];
    }
};

// ============================================================================
// SchemaCache
// ============================================================================

pub const SchemaCache = struct {
    attrs: [MAX_ATTRS]SchemaAttr = undefined,
    count: u16 = 0,

    /// Load schema attributes from an EAV B+ tree.
    /// Scans all partition-0 entities and builds SchemaAttr entries.
    pub fn load(eav_tree: btree.BPlusTree) SchemaCache {
        var cache = SchemaCache{};
        cache.count = 0;

        var it = eav_tree.seekFirst();

        // Current entity being assembled
        var cur_eid: u64 = 0;
        var cur_attr: SchemaAttr = undefined;
        var have_entity = false;

        while (it.next()) |entry| {
            const eid = decodeEavEntity(entry.key);

            // Only process partition-0 entities (schema)
            if (partitionOf(eid) != PARTITION_DB) break;

            if (eid != cur_eid) {
                // Flush previous entity
                if (have_entity and cur_attr.ident_len > 0) {
                    if (cache.count < MAX_ATTRS) {
                        cache.attrs[cache.count] = cur_attr;
                        cache.count += 1;
                    }
                }
                cur_eid = eid;
                cur_attr = SchemaAttr{ .eid = eid };
                have_entity = true;
            }

            const attr_id = decodeEavAttr(entry.key);
            const val = decodeEavValue(entry.key);

            switch (attr_id) {
                ATTR_IDENT => {
                    const kw = val.keyword;
                    const len: u8 = @intCast(@min(kw.len, MAX_IDENT_LEN));
                    @memcpy(cur_attr.ident_buf[0..len], kw[0..len]);
                    cur_attr.ident_len = len;
                },
                ATTR_VALUE_TYPE => {
                    cur_attr.value_type = keywordToTag(val.keyword);
                },
                ATTR_CARDINALITY => {
                    cur_attr.cardinality = keywordToCardinality(val.keyword);
                },
                ATTR_UNIQUE => {
                    cur_attr.unique = keywordToUniqueness(val.keyword);
                },
                ATTR_INDEX => {
                    cur_attr.indexed = val.boolean;
                },
                ATTR_IS_COMPONENT => {
                    cur_attr.is_component = val.boolean;
                },
                else => {},
            }
        }

        // Flush last entity
        if (have_entity and cur_attr.ident_len > 0) {
            if (cache.count < MAX_ATTRS) {
                cache.attrs[cache.count] = cur_attr;
                cache.count += 1;
            }
        }

        return cache;
    }

    /// Resolve a keyword ident to its entity ID. Returns null if not found.
    pub fn resolveIdent(self: *const SchemaCache, keyword: []const u8) ?u64 {
        for (self.attrs[0..self.count]) |*attr| {
            if (mem.eql(u8, attr.ident(), keyword)) return attr.eid;
        }
        return null;
    }

    /// Get a schema attribute by entity ID. Returns null if not found.
    pub fn getAttr(self: *const SchemaCache, eid: u64) ?*const SchemaAttr {
        for (&self.attrs, 0..) |*attr, i| {
            if (i >= self.count) break;
            if (attr.eid == eid) return attr;
        }
        return null;
    }

    /// Validate that a value's type matches the attribute's declared valueType.
    pub fn validateType(self: *const SchemaCache, attr_eid: u64, value: encoding.Value) bool {
        const attr = self.getAttr(attr_eid) orelse return false;
        return std.meta.activeTag(value) == attr.value_type;
    }

    /// Check if an attribute should be indexed in AVE.
    /// True when explicitly indexed or has a uniqueness constraint.
    pub fn isIndexed(self: *const SchemaCache, attr_eid: u64) bool {
        const attr = self.getAttr(attr_eid) orelse return false;
        return attr.indexed or attr.unique != .none;
    }

    /// Check if an attribute's value type is ref (for VAE index).
    pub fn isRef(self: *const SchemaCache, attr_eid: u64) bool {
        const attr = self.getAttr(attr_eid) orelse return false;
        return attr.value_type == .ref;
    }
};

// ============================================================================
// Keyword → enum converters
// ============================================================================

fn keywordToTag(kw: []const u8) encoding.Tag {
    if (mem.eql(u8, kw, ":db.type/boolean")) return .boolean;
    if (mem.eql(u8, kw, ":db.type/long")) return .i64;
    if (mem.eql(u8, kw, ":db.type/double")) return .f64;
    if (mem.eql(u8, kw, ":db.type/string")) return .string;
    if (mem.eql(u8, kw, ":db.type/keyword")) return .keyword;
    if (mem.eql(u8, kw, ":db.type/ref")) return .ref;
    if (mem.eql(u8, kw, ":db.type/instant")) return .instant;
    if (mem.eql(u8, kw, ":db.type/uuid")) return .uuid;
    if (mem.eql(u8, kw, ":db.type/bytes")) return .bytes;
    return .nil;
}

fn tagToKeyword(tag: encoding.Tag) []const u8 {
    return switch (tag) {
        .nil => ":db.type/nil",
        .boolean => ":db.type/boolean",
        .i64 => ":db.type/long",
        .f64 => ":db.type/double",
        .string => ":db.type/string",
        .keyword => ":db.type/keyword",
        .ref => ":db.type/ref",
        .instant => ":db.type/instant",
        .uuid => ":db.type/uuid",
        .bytes => ":db.type/bytes",
    };
}

fn keywordToCardinality(kw: []const u8) Cardinality {
    if (mem.eql(u8, kw, ":db.cardinality/many")) return .many;
    return .one;
}

fn cardinalityToKeyword(c: Cardinality) []const u8 {
    return switch (c) {
        .one => ":db.cardinality/one",
        .many => ":db.cardinality/many",
    };
}

fn keywordToUniqueness(kw: []const u8) Uniqueness {
    if (mem.eql(u8, kw, ":db.unique/value")) return .value;
    if (mem.eql(u8, kw, ":db.unique/identity")) return .identity;
    return .none;
}

fn uniquenessToKeyword(u: Uniqueness) []const u8 {
    return switch (u) {
        .none => ":db.unique/none",
        .value => ":db.unique/value",
        .identity => ":db.unique/identity",
    };
}

// ============================================================================
// Bootstrap
// ============================================================================

const Datom = struct { e: u64, a: u64, v: encoding.Value };

/// Bootstrap datoms that define the 8 meta-schema attributes.
pub const BOOTSTRAP_DATOMS = [_]Datom{
    // 1: :db/ident — keyword, cardinality/one, unique/identity
    .{ .e = 1, .a = ATTR_IDENT, .v = .{ .keyword = ":db/ident" } },
    .{ .e = 1, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/keyword" } },
    .{ .e = 1, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
    .{ .e = 1, .a = ATTR_UNIQUE, .v = .{ .keyword = ":db.unique/identity" } },
    // 2: :db/valueType — keyword, cardinality/one
    .{ .e = 2, .a = ATTR_IDENT, .v = .{ .keyword = ":db/valueType" } },
    .{ .e = 2, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/keyword" } },
    .{ .e = 2, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
    // 3: :db/cardinality — keyword, cardinality/one
    .{ .e = 3, .a = ATTR_IDENT, .v = .{ .keyword = ":db/cardinality" } },
    .{ .e = 3, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/keyword" } },
    .{ .e = 3, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
    // 4: :db/unique — keyword, cardinality/one
    .{ .e = 4, .a = ATTR_IDENT, .v = .{ .keyword = ":db/unique" } },
    .{ .e = 4, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/keyword" } },
    .{ .e = 4, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
    // 5: :db/index — boolean, cardinality/one
    .{ .e = 5, .a = ATTR_IDENT, .v = .{ .keyword = ":db/index" } },
    .{ .e = 5, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/boolean" } },
    .{ .e = 5, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
    // 6: :db/isComponent — boolean, cardinality/one
    .{ .e = 6, .a = ATTR_IDENT, .v = .{ .keyword = ":db/isComponent" } },
    .{ .e = 6, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/boolean" } },
    .{ .e = 6, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
    // 7: :db/doc — string, cardinality/one
    .{ .e = 7, .a = ATTR_IDENT, .v = .{ .keyword = ":db/doc" } },
    .{ .e = 7, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/string" } },
    .{ .e = 7, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
    // 8: :db/txInstant — instant, cardinality/one
    .{ .e = 8, .a = ATTR_IDENT, .v = .{ .keyword = ":db/txInstant" } },
    .{ .e = 8, .a = ATTR_VALUE_TYPE, .v = .{ .keyword = ":db.type/instant" } },
    .{ .e = 8, .a = ATTR_CARDINALITY, .v = .{ .keyword = ":db.cardinality/one" } },
};

pub const BootstrapResult = struct {
    eav_root: u64,
    next_entity_id: u64,
};

/// Bootstrap the schema: insert all meta-schema datoms into a fresh EAV B+ tree.
pub fn bootstrap(fm: *file.FileManager) !BootstrapResult {
    var tree = btree.BPlusTree.init(0, fm, eavKeyCmp);

    var key_buf: [512]u8 = undefined;
    for (BOOTSTRAP_DATOMS) |datom| {
        const key_len = encodeEavKey(&key_buf, datom.e, datom.a, datom.v);
        try tree.insert(key_buf[0..key_len], &.{});
    }

    return .{
        .eav_root = tree.root,
        .next_entity_id = 9, // next available after bootstrap entities 1-8
    };
}

// ============================================================================
// Tests
// ============================================================================

test "EAV key encode/decode roundtrip" {
    var buf: [512]u8 = undefined;
    const key_len = encodeEavKey(&buf, 42, ATTR_IDENT, .{ .keyword = ":db/ident" });
    const key = buf[0..key_len];

    try testing.expectEqual(@as(u64, 42), decodeEavEntity(key));
    try testing.expectEqual(@as(u64, ATTR_IDENT), decodeEavAttr(key));
    const val = decodeEavValue(key);
    try testing.expectEqualStrings(":db/ident", val.keyword);
}

test "EAV key ordering: entity first, then attribute, then value" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;

    // Different entities: lower entity sorts first
    const la = encodeEavKey(&buf_a, 1, 1, .{ .keyword = ":z" });
    const lb = encodeEavKey(&buf_b, 2, 1, .{ .keyword = ":a" });
    try testing.expectEqual(math.Order.lt, eavKeyCmp(buf_a[0..la], buf_b[0..lb]));

    // Same entity, different attributes
    const la2 = encodeEavKey(&buf_a, 1, 1, .{ .keyword = ":z" });
    const lb2 = encodeEavKey(&buf_b, 1, 2, .{ .keyword = ":a" });
    try testing.expectEqual(math.Order.lt, eavKeyCmp(buf_a[0..la2], buf_b[0..lb2]));

    // Same entity+attr, different values
    const la3 = encodeEavKey(&buf_a, 1, 1, .{ .keyword = ":aaa" });
    const lb3 = encodeEavKey(&buf_b, 1, 1, .{ .keyword = ":bbb" });
    try testing.expectEqual(math.Order.lt, eavKeyCmp(buf_a[0..la3], buf_b[0..lb3]));
}

test "entity ID helpers" {
    try testing.expectEqual(@as(u64, 1), makeEntityId(0, 1));
    try testing.expectEqual(@as(u64, (@as(u64, 2) << 54) | 1), makeEntityId(2, 1));
    try testing.expectEqual(@as(u10, 0), partitionOf(1));
    try testing.expectEqual(@as(u10, 0), partitionOf(8));
    try testing.expectEqual(@as(u10, 2), partitionOf(makeEntityId(2, 1)));
    try testing.expectEqual(@as(u10, 1), partitionOf(makeEntityId(1, 42)));
}

test "bootstrap creates valid EAV tree" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const result = try bootstrap(&fm);
    try testing.expect(result.eav_root != 0);
    try testing.expectEqual(@as(u64, 9), result.next_entity_id);

    // Verify all 8 schema entities are findable via seek
    const tree = btree.BPlusTree.init(result.eav_root, &fm, eavKeyCmp);
    var key_buf: [512]u8 = undefined;

    // Check entity 1 has :db/ident datom
    const k1 = encodeEavKey(&key_buf, 1, ATTR_IDENT, .{ .keyword = ":db/ident" });
    const val1 = tree.lookup(key_buf[0..k1]);
    try testing.expect(val1 != null);

    // Check entity 8 has :db/txInstant datom
    const k8 = encodeEavKey(&key_buf, 8, ATTR_IDENT, .{ .keyword = ":db/txInstant" });
    const val8 = tree.lookup(key_buf[0..k8]);
    try testing.expect(val8 != null);
}

test "SchemaCache load from bootstrap" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const result = try bootstrap(&fm);
    const tree = btree.BPlusTree.init(result.eav_root, &fm, eavKeyCmp);
    const cache = SchemaCache.load(tree);

    try testing.expectEqual(@as(u16, 8), cache.count);

    // Verify attr 1: :db/ident
    const a1 = cache.getAttr(1).?;
    try testing.expectEqualStrings(":db/ident", a1.ident());
    try testing.expectEqual(encoding.Tag.keyword, a1.value_type);
    try testing.expectEqual(Cardinality.one, a1.cardinality);
    try testing.expectEqual(Uniqueness.identity, a1.unique);

    // Verify attr 2: :db/valueType
    const a2 = cache.getAttr(2).?;
    try testing.expectEqualStrings(":db/valueType", a2.ident());
    try testing.expectEqual(encoding.Tag.keyword, a2.value_type);

    // Verify attr 5: :db/index (boolean type)
    const a5 = cache.getAttr(5).?;
    try testing.expectEqualStrings(":db/index", a5.ident());
    try testing.expectEqual(encoding.Tag.boolean, a5.value_type);

    // Verify attr 7: :db/doc (string type)
    const a7 = cache.getAttr(7).?;
    try testing.expectEqualStrings(":db/doc", a7.ident());
    try testing.expectEqual(encoding.Tag.string, a7.value_type);

    // Verify attr 8: :db/txInstant (instant type)
    const a8 = cache.getAttr(8).?;
    try testing.expectEqualStrings(":db/txInstant", a8.ident());
    try testing.expectEqual(encoding.Tag.instant, a8.value_type);
}

test "resolveIdent" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const result = try bootstrap(&fm);
    const tree = btree.BPlusTree.init(result.eav_root, &fm, eavKeyCmp);
    const cache = SchemaCache.load(tree);

    try testing.expectEqual(@as(?u64, 1), cache.resolveIdent(":db/ident"));
    try testing.expectEqual(@as(?u64, 2), cache.resolveIdent(":db/valueType"));
    try testing.expectEqual(@as(?u64, 8), cache.resolveIdent(":db/txInstant"));
    try testing.expectEqual(@as(?u64, null), cache.resolveIdent(":nonexistent"));
}

test "getAttr returns correct metadata" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const result = try bootstrap(&fm);
    const tree = btree.BPlusTree.init(result.eav_root, &fm, eavKeyCmp);
    const cache = SchemaCache.load(tree);

    const a1 = cache.getAttr(1).?;
    try testing.expectEqualStrings(":db/ident", a1.ident());
    try testing.expectEqual(encoding.Tag.keyword, a1.value_type);
    try testing.expectEqual(Cardinality.one, a1.cardinality);
    try testing.expectEqual(Uniqueness.identity, a1.unique);

    // Non-existent
    try testing.expectEqual(@as(?*const SchemaAttr, null), cache.getAttr(999));
}

test "validateType accepts correct type" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const result = try bootstrap(&fm);
    const tree = btree.BPlusTree.init(result.eav_root, &fm, eavKeyCmp);
    const cache = SchemaCache.load(tree);

    // :db/ident is keyword type — should accept keyword
    try testing.expect(cache.validateType(1, .{ .keyword = ":test" }));
    // :db/index is boolean type — should accept boolean
    try testing.expect(cache.validateType(5, .{ .boolean = true }));
    // :db/doc is string type — should accept string
    try testing.expect(cache.validateType(7, .{ .string = "hello" }));
    // :db/txInstant is instant type — should accept instant
    try testing.expect(cache.validateType(8, .{ .instant = 12345 }));
}

test "validateType rejects wrong type" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const result = try bootstrap(&fm);
    const tree = btree.BPlusTree.init(result.eav_root, &fm, eavKeyCmp);
    const cache = SchemaCache.load(tree);

    // :db/ident is keyword — reject i64
    try testing.expect(!cache.validateType(1, .{ .i64 = 42 }));
    // :db/ident is keyword — reject boolean
    try testing.expect(!cache.validateType(1, .{ .boolean = true }));
    // :db/index is boolean — reject keyword
    try testing.expect(!cache.validateType(5, .{ .keyword = ":nope" }));
    // non-existent attr — reject anything
    try testing.expect(!cache.validateType(999, .{ .keyword = ":x" }));
}

test "user-defined schema: insert new attr, reload cache" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const result = try bootstrap(&fm);
    var tree = btree.BPlusTree.init(result.eav_root, &fm, eavKeyCmp);

    // Add a user-defined schema attribute (entity 9, still partition 0)
    var key_buf: [512]u8 = undefined;
    var kl = encodeEavKey(&key_buf, 9, ATTR_IDENT, .{ .keyword = ":user/name" });
    try tree.insert(key_buf[0..kl], &.{});

    kl = encodeEavKey(&key_buf, 9, ATTR_VALUE_TYPE, .{ .keyword = ":db.type/string" });
    try tree.insert(key_buf[0..kl], &.{});

    kl = encodeEavKey(&key_buf, 9, ATTR_CARDINALITY, .{ .keyword = ":db.cardinality/one" });
    try tree.insert(key_buf[0..kl], &.{});

    // Reload cache
    const cache = SchemaCache.load(tree);
    try testing.expectEqual(@as(u16, 9), cache.count);

    const a9 = cache.getAttr(9).?;
    try testing.expectEqualStrings(":user/name", a9.ident());
    try testing.expectEqual(encoding.Tag.string, a9.value_type);
    try testing.expectEqual(Cardinality.one, a9.cardinality);

    try testing.expectEqual(@as(?u64, 9), cache.resolveIdent(":user/name"));
    try testing.expect(cache.validateType(9, .{ .string = "Alice" }));
    try testing.expect(!cache.validateType(9, .{ .i64 = 42 }));
}
