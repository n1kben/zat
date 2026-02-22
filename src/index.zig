// ZatDB — Index Manager
//
// Routes datoms to EAV, AVE, VAE, and TxLog B+ tree indexes based on
// schema metadata. Each index has its own key encoding and comparison
// function; all use big-endian fields so plain memcmp provides correct
// sort order.
//
// Key encodings (flat composite keys, empty B+ tree values):
//   EAV:   [E:8 BE][A:8 BE][encoded_value]          — primary entity lookup
//   AVE:   [A:8 BE][encoded_value][E:8 BE]           — value lookup (selective)
//   VAE:   [V(ref):8 BE][A:8 BE][E:8 BE]             — reverse ref traversal
//   TxLog: [Tx:8 BE][E:8 BE][A:8 BE][encoded_value][Op:1] — time travel

const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;
const encoding = @import("encoding.zig");
const btree = @import("btree.zig");
const file = @import("file.zig");
const page = @import("page.zig");
const schema = @import("schema.zig");

// ============================================================================
// Datom
// ============================================================================

pub const Datom = struct {
    e: u64,
    a: u64,
    v: encoding.Value,
    tx: u64,
    op: bool, // true = assert, false = retract
};

// ============================================================================
// Index Roots
// ============================================================================

pub const IndexRoots = struct {
    eav: u64 = 0,
    ave: u64 = 0,
    vae: u64 = 0,
    txlog: u64 = 0,
};

// ============================================================================
// AVE Key Encoding: [A:8 BE][encoded_value:variable][E:8 BE]
// ============================================================================

pub fn encodeAveKey(buf: []u8, attr: u64, value: encoding.Value, entity: u64) usize {
    encoding.writeU64(buf[0..8], attr);
    const val_len = encoding.encode(value, buf[8..]);
    encoding.writeU64(buf[8 + val_len ..][0..8], entity);
    return 8 + val_len + 8;
}

pub fn decodeAveAttr(key: []const u8) u64 {
    return encoding.readU64(key[0..8]);
}

pub fn decodeAveEntity(key: []const u8) u64 {
    return encoding.readU64(key[key.len - 8 ..][0..8]);
}

pub fn decodeAveValue(key: []const u8) encoding.Value {
    return encoding.decode(key[8 .. key.len - 8]);
}

/// AVE keys use plain memcmp (all components are big-endian sortable).
pub fn aveKeyCmp(a: []const u8, b: []const u8) math.Order {
    return mem.order(u8, a, b);
}

// ============================================================================
// VAE Key Encoding: [V(ref):8 BE][A:8 BE][E:8 BE]
// ============================================================================

pub const VAE_KEY_SIZE = 24;

pub fn encodeVaeKey(buf: []u8, ref_val: u64, attr: u64, entity: u64) usize {
    encoding.writeU64(buf[0..8], ref_val);
    encoding.writeU64(buf[8..16], attr);
    encoding.writeU64(buf[16..24], entity);
    return VAE_KEY_SIZE;
}

pub fn decodeVaeRef(key: []const u8) u64 {
    return encoding.readU64(key[0..8]);
}

pub fn decodeVaeAttr(key: []const u8) u64 {
    return encoding.readU64(key[8..16]);
}

pub fn decodeVaeEntity(key: []const u8) u64 {
    return encoding.readU64(key[16..24]);
}

/// VAE keys use plain memcmp.
pub fn vaeKeyCmp(a: []const u8, b: []const u8) math.Order {
    return mem.order(u8, a, b);
}

// ============================================================================
// TxLog Key Encoding: [Tx:8 BE][E:8 BE][A:8 BE][encoded_value:variable][Op:1]
// ============================================================================

pub fn encodeTxLogKey(buf: []u8, tx: u64, entity: u64, attr: u64, value: encoding.Value, op: bool) usize {
    encoding.writeU64(buf[0..8], tx);
    encoding.writeU64(buf[8..16], entity);
    encoding.writeU64(buf[16..24], attr);
    const val_len = encoding.encode(value, buf[24..]);
    buf[24 + val_len] = if (op) 0x01 else 0x00;
    return 24 + val_len + 1;
}

pub fn decodeTxLogTx(key: []const u8) u64 {
    return encoding.readU64(key[0..8]);
}

pub fn decodeTxLogEntity(key: []const u8) u64 {
    return encoding.readU64(key[8..16]);
}

pub fn decodeTxLogAttr(key: []const u8) u64 {
    return encoding.readU64(key[16..24]);
}

pub fn decodeTxLogValue(key: []const u8) encoding.Value {
    return encoding.decode(key[24 .. key.len - 1]);
}

pub fn decodeTxLogOp(key: []const u8) bool {
    return key[key.len - 1] == 0x01;
}

/// TxLog keys use plain memcmp.
pub fn txLogKeyCmp(a: []const u8, b: []const u8) math.Order {
    return mem.order(u8, a, b);
}

// ============================================================================
// IndexManager
// ============================================================================

pub const IndexManager = struct {
    eav: btree.BPlusTree,
    ave: btree.BPlusTree,
    vae: btree.BPlusTree,
    txlog: btree.BPlusTree,
    schema_cache: schema.SchemaCache,

    pub fn init(
        roots: IndexRoots,
        fm: *file.FileManager,
        cache: schema.SchemaCache,
    ) IndexManager {
        var mgr = IndexManager{
            .eav = btree.BPlusTree.init(roots.eav, fm, schema.eavKeyCmp),
            .ave = btree.BPlusTree.init(roots.ave, fm, aveKeyCmp),
            .vae = btree.BPlusTree.init(roots.vae, fm, vaeKeyCmp),
            .txlog = btree.BPlusTree.init(roots.txlog, fm, txLogKeyCmp),
            .schema_cache = cache,
        };
        mgr.ave.index_id = .ave;
        mgr.vae.index_id = .vae;
        mgr.txlog.index_id = .tx_log;
        return mgr;
    }

    /// Return current root page IDs for all indexes.
    pub fn currentRoots(self: *const IndexManager) IndexRoots {
        return .{
            .eav = self.eav.root,
            .ave = self.ave.root,
            .vae = self.vae.root,
            .txlog = self.txlog.root,
        };
    }

    /// Insert a datom into all relevant indexes and return new root pages.
    ///
    /// Routing rules:
    ///   - ALL datoms → EAV + TxLog
    ///   - Indexed/unique attributes → AVE
    ///   - Ref-typed attributes → VAE
    pub fn insertDatom(self: *IndexManager, d: Datom) !IndexRoots {
        var key_buf: [512]u8 = undefined;

        // Always insert into EAV
        const eav_len = schema.encodeEavKey(&key_buf, d.e, d.a, d.v);
        try self.eav.insert(key_buf[0..eav_len], &.{});

        // Always insert into TxLog
        const txlog_len = encodeTxLogKey(&key_buf, d.tx, d.e, d.a, d.v, d.op);
        try self.txlog.insert(key_buf[0..txlog_len], &.{});

        // AVE: only for indexed or unique attributes
        if (self.schema_cache.isIndexed(d.a)) {
            const ave_len = encodeAveKey(&key_buf, d.a, d.v, d.e);
            try self.ave.insert(key_buf[0..ave_len], &.{});
        }

        // VAE: only for ref-typed attributes
        if (self.schema_cache.isRef(d.a)) {
            switch (d.v) {
                .ref => |ref_val| {
                    const vae_len = encodeVaeKey(&key_buf, ref_val, d.a, d.e);
                    try self.vae.insert(key_buf[0..vae_len], &.{});
                },
                else => {},
            }
        }

        return self.currentRoots();
    }
};

// ============================================================================
// Tests
// ============================================================================

// -- Test helpers -----------------------------------------------------------

/// User-partition entity ID helper.
fn userEntity(seq: u54) u64 {
    return schema.makeEntityId(schema.PARTITION_USER, seq);
}

const USER_ATTR_NAME: u64 = 9; // :user/name — string, not indexed
const USER_ATTR_EMAIL: u64 = 10; // :user/email — string, unique/identity
const USER_ATTR_FRIEND: u64 = 11; // :user/friend — ref, indexed

/// Bootstrap + add user schema attributes + load cache.
fn setupTestDb(fm: *file.FileManager) !struct { cache: schema.SchemaCache, eav_root: u64 } {
    const boot = try schema.bootstrap(fm);
    var tree = btree.BPlusTree.init(boot.eav_root, fm, schema.eavKeyCmp);

    var buf: [512]u8 = undefined;

    // Entity 9: :user/name — string, cardinality/one (NOT indexed)
    var kl = schema.encodeEavKey(&buf, 9, schema.ATTR_IDENT, .{ .keyword = ":user/name" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 9, schema.ATTR_VALUE_TYPE, .{ .keyword = ":db.type/string" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 9, schema.ATTR_CARDINALITY, .{ .keyword = ":db.cardinality/one" });
    try tree.insert(buf[0..kl], &.{});

    // Entity 10: :user/email — string, cardinality/one, unique/identity
    kl = schema.encodeEavKey(&buf, 10, schema.ATTR_IDENT, .{ .keyword = ":user/email" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 10, schema.ATTR_VALUE_TYPE, .{ .keyword = ":db.type/string" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 10, schema.ATTR_CARDINALITY, .{ .keyword = ":db.cardinality/one" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 10, schema.ATTR_UNIQUE, .{ .keyword = ":db.unique/identity" });
    try tree.insert(buf[0..kl], &.{});

    // Entity 11: :user/friend — ref, cardinality/many, indexed
    kl = schema.encodeEavKey(&buf, 11, schema.ATTR_IDENT, .{ .keyword = ":user/friend" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 11, schema.ATTR_VALUE_TYPE, .{ .keyword = ":db.type/ref" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 11, schema.ATTR_CARDINALITY, .{ .keyword = ":db.cardinality/many" });
    try tree.insert(buf[0..kl], &.{});
    kl = schema.encodeEavKey(&buf, 11, schema.ATTR_INDEX, .{ .boolean = true });
    try tree.insert(buf[0..kl], &.{});

    const cache = schema.SchemaCache.load(tree);
    return .{ .cache = cache, .eav_root = tree.root };
}

// -- Encoding roundtrip tests -----------------------------------------------

test "AVE key encode/decode roundtrip" {
    var buf: [512]u8 = undefined;
    const kl = encodeAveKey(&buf, 42, .{ .string = "alice@example.com" }, 1001);
    const key = buf[0..kl];

    try testing.expectEqual(@as(u64, 42), decodeAveAttr(key));
    try testing.expectEqual(@as(u64, 1001), decodeAveEntity(key));
    const val = decodeAveValue(key);
    try testing.expectEqualStrings("alice@example.com", val.string);
}

test "AVE key ordering: attribute first, then value, then entity" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;

    // Different attributes
    const la = encodeAveKey(&buf_a, 1, .{ .string = "z" }, 1);
    const lb = encodeAveKey(&buf_b, 2, .{ .string = "a" }, 1);
    try testing.expectEqual(math.Order.lt, aveKeyCmp(buf_a[0..la], buf_b[0..lb]));

    // Same attr, different values
    const la2 = encodeAveKey(&buf_a, 1, .{ .string = "aaa" }, 1);
    const lb2 = encodeAveKey(&buf_b, 1, .{ .string = "bbb" }, 1);
    try testing.expectEqual(math.Order.lt, aveKeyCmp(buf_a[0..la2], buf_b[0..lb2]));

    // Same attr+value, different entities
    const la3 = encodeAveKey(&buf_a, 1, .{ .string = "same" }, 1);
    const lb3 = encodeAveKey(&buf_b, 1, .{ .string = "same" }, 2);
    try testing.expectEqual(math.Order.lt, aveKeyCmp(buf_a[0..la3], buf_b[0..lb3]));
}

test "VAE key encode/decode roundtrip" {
    var buf: [512]u8 = undefined;
    const kl = encodeVaeKey(&buf, 2000, 11, 1001);
    const key = buf[0..kl];

    try testing.expectEqual(@as(u64, 2000), decodeVaeRef(key));
    try testing.expectEqual(@as(u64, 11), decodeVaeAttr(key));
    try testing.expectEqual(@as(u64, 1001), decodeVaeEntity(key));
    try testing.expectEqual(@as(usize, VAE_KEY_SIZE), kl);
}

test "VAE key ordering: ref value first, then attribute, then entity" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;

    // Different ref values
    _ = encodeVaeKey(&buf_a, 1, 11, 1001);
    _ = encodeVaeKey(&buf_b, 2, 11, 1001);
    try testing.expectEqual(math.Order.lt, vaeKeyCmp(buf_a[0..VAE_KEY_SIZE], buf_b[0..VAE_KEY_SIZE]));

    // Same ref, different attrs
    _ = encodeVaeKey(&buf_a, 1, 10, 1001);
    _ = encodeVaeKey(&buf_b, 1, 11, 1001);
    try testing.expectEqual(math.Order.lt, vaeKeyCmp(buf_a[0..VAE_KEY_SIZE], buf_b[0..VAE_KEY_SIZE]));

    // Same ref+attr, different entities
    _ = encodeVaeKey(&buf_a, 1, 11, 1000);
    _ = encodeVaeKey(&buf_b, 1, 11, 1001);
    try testing.expectEqual(math.Order.lt, vaeKeyCmp(buf_a[0..VAE_KEY_SIZE], buf_b[0..VAE_KEY_SIZE]));
}

test "TxLog key encode/decode roundtrip" {
    var buf: [512]u8 = undefined;
    const kl = encodeTxLogKey(&buf, 100, 42, 9, .{ .string = "Alice" }, true);
    const key = buf[0..kl];

    try testing.expectEqual(@as(u64, 100), decodeTxLogTx(key));
    try testing.expectEqual(@as(u64, 42), decodeTxLogEntity(key));
    try testing.expectEqual(@as(u64, 9), decodeTxLogAttr(key));
    try testing.expectEqualStrings("Alice", decodeTxLogValue(key).string);
    try testing.expect(decodeTxLogOp(key));
}

test "TxLog key encode/decode retraction" {
    var buf: [512]u8 = undefined;
    const kl = encodeTxLogKey(&buf, 50, 42, 9, .{ .string = "Alice" }, false);
    const key = buf[0..kl];

    try testing.expectEqual(@as(u64, 50), decodeTxLogTx(key));
    try testing.expect(!decodeTxLogOp(key));
}

test "TxLog key ordering: tx first, then entity, attr, value, op" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;

    // Different tx
    const la = encodeTxLogKey(&buf_a, 1, 42, 9, .{ .string = "A" }, true);
    const lb = encodeTxLogKey(&buf_b, 2, 42, 9, .{ .string = "A" }, true);
    try testing.expectEqual(math.Order.lt, txLogKeyCmp(buf_a[0..la], buf_b[0..lb]));

    // Same tx, different entity
    const la2 = encodeTxLogKey(&buf_a, 1, 1, 9, .{ .string = "A" }, true);
    const lb2 = encodeTxLogKey(&buf_b, 1, 2, 9, .{ .string = "A" }, true);
    try testing.expectEqual(math.Order.lt, txLogKeyCmp(buf_a[0..la2], buf_b[0..lb2]));
}

// -- IndexManager routing tests ---------------------------------------------

test "datom appears in correct indexes — indexed ref attr (all four)" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    // :user/friend is ref + indexed → should go to EAV, AVE, VAE, TxLog
    const e1 = userEntity(1);
    const e2 = userEntity(2);
    _ = try mgr.insertDatom(.{
        .e = e1,
        .a = USER_ATTR_FRIEND,
        .v = .{ .ref = e2 },
        .tx = 100,
        .op = true,
    });

    // Verify EAV: lookup by entity+attr+value
    var eav_buf: [512]u8 = undefined;
    const eav_kl = schema.encodeEavKey(&eav_buf, e1, USER_ATTR_FRIEND, .{ .ref = e2 });
    try testing.expect(mgr.eav.lookup(eav_buf[0..eav_kl]) != null);

    // Verify AVE: lookup by attr+value+entity
    var ave_buf: [512]u8 = undefined;
    const ave_kl = encodeAveKey(&ave_buf, USER_ATTR_FRIEND, .{ .ref = e2 }, e1);
    try testing.expect(mgr.ave.lookup(ave_buf[0..ave_kl]) != null);

    // Verify VAE: lookup by ref+attr+entity
    var vae_buf: [512]u8 = undefined;
    _ = encodeVaeKey(&vae_buf, e2, USER_ATTR_FRIEND, e1);
    try testing.expect(mgr.vae.lookup(vae_buf[0..VAE_KEY_SIZE]) != null);

    // Verify TxLog: lookup by tx+entity+attr+value+op
    var txlog_buf: [512]u8 = undefined;
    const txlog_kl = encodeTxLogKey(&txlog_buf, 100, e1, USER_ATTR_FRIEND, .{ .ref = e2 }, true);
    try testing.expect(mgr.txlog.lookup(txlog_buf[0..txlog_kl]) != null);
}

test "datom appears in correct indexes — non-indexed non-ref attr (EAV + TxLog only)" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    // :user/name is string, not indexed → should go to EAV + TxLog only
    const e1 = userEntity(1);
    _ = try mgr.insertDatom(.{
        .e = e1,
        .a = USER_ATTR_NAME,
        .v = .{ .string = "Alice" },
        .tx = 100,
        .op = true,
    });

    // Verify EAV: present
    var eav_buf: [512]u8 = undefined;
    const eav_kl = schema.encodeEavKey(&eav_buf, e1, USER_ATTR_NAME, .{ .string = "Alice" });
    try testing.expect(mgr.eav.lookup(eav_buf[0..eav_kl]) != null);

    // Verify TxLog: present
    var txlog_buf: [512]u8 = undefined;
    const txlog_kl = encodeTxLogKey(&txlog_buf, 100, e1, USER_ATTR_NAME, .{ .string = "Alice" }, true);
    try testing.expect(mgr.txlog.lookup(txlog_buf[0..txlog_kl]) != null);

    // Verify AVE: NOT present
    try testing.expectEqual(@as(u64, 0), mgr.ave.root);

    // Verify VAE: NOT present
    try testing.expectEqual(@as(u64, 0), mgr.vae.root);
}

test "EAV lookup by entity — multiple attributes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    const e42 = userEntity(42);
    _ = try mgr.insertDatom(.{ .e = e42, .a = USER_ATTR_NAME, .v = .{ .string = "Alice" }, .tx = 100, .op = true });
    _ = try mgr.insertDatom(.{ .e = e42, .a = USER_ATTR_EMAIL, .v = .{ .string = "alice@example.com" }, .tx = 100, .op = true });

    // Seek by entity prefix — should find both attributes
    var prefix: [8]u8 = undefined;
    encoding.writeU64(&prefix, e42);
    var it = mgr.eav.seek(&prefix);

    // First datom
    const d1 = it.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(e42, schema.decodeEavEntity(d1.key));

    // Second datom
    const d2 = it.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(e42, schema.decodeEavEntity(d2.key));

    // Collect the two attribute IDs
    const a1 = schema.decodeEavAttr(d1.key);
    const a2 = schema.decodeEavAttr(d2.key);
    try testing.expect((a1 == USER_ATTR_NAME and a2 == USER_ATTR_EMAIL) or
        (a1 == USER_ATTR_EMAIL and a2 == USER_ATTR_NAME));
}

test "AVE lookup by value — unique attribute" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    const e1 = userEntity(1);
    _ = try mgr.insertDatom(.{
        .e = e1,
        .a = USER_ATTR_EMAIL,
        .v = .{ .string = "alice@example.com" },
        .tx = 100,
        .op = true,
    });

    // AVE seek by attr + value prefix → should find entity
    var ave_prefix: [512]u8 = undefined;
    encoding.writeU64(ave_prefix[0..8], USER_ATTR_EMAIL);
    const val_len = encoding.encode(.{ .string = "alice@example.com" }, ave_prefix[8..]);
    const prefix_len = 8 + val_len;

    var it = mgr.ave.seek(ave_prefix[0..prefix_len]);
    const entry = it.next() orelse return error.TestUnexpectedResult;

    try testing.expectEqual(USER_ATTR_EMAIL, decodeAveAttr(entry.key));
    try testing.expectEqual(e1, decodeAveEntity(entry.key));
    try testing.expectEqualStrings("alice@example.com", decodeAveValue(entry.key).string);
}

test "VAE reverse reference traversal" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    const e1 = userEntity(1);
    const e2 = userEntity(2);
    _ = try mgr.insertDatom(.{
        .e = e1,
        .a = USER_ATTR_FRIEND,
        .v = .{ .ref = e2 },
        .tx = 100,
        .op = true,
    });

    // VAE seek by ref value + attr → should find the referring entity
    var vae_prefix: [16]u8 = undefined;
    encoding.writeU64(vae_prefix[0..8], e2);
    encoding.writeU64(vae_prefix[8..16], USER_ATTR_FRIEND);

    var it = mgr.vae.seek(&vae_prefix);
    const entry = it.next() orelse return error.TestUnexpectedResult;

    try testing.expectEqual(e2, decodeVaeRef(entry.key));
    try testing.expectEqual(USER_ATTR_FRIEND, decodeVaeAttr(entry.key));
    try testing.expectEqual(e1, decodeVaeEntity(entry.key));
}

test "unique attr goes to AVE but not VAE" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    // :user/email is unique/identity (string, not ref)
    const e1 = userEntity(1);
    _ = try mgr.insertDatom(.{
        .e = e1,
        .a = USER_ATTR_EMAIL,
        .v = .{ .string = "alice@example.com" },
        .tx = 100,
        .op = true,
    });

    // AVE should have it
    try testing.expect(mgr.ave.root != 0);

    // VAE should NOT (not a ref)
    try testing.expectEqual(@as(u64, 0), mgr.vae.root);
}

test "multiple datoms accumulate across indexes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    const e1 = userEntity(1);
    const e2 = userEntity(2);
    const e3 = userEntity(3);

    // Insert several datoms
    _ = try mgr.insertDatom(.{ .e = e1, .a = USER_ATTR_NAME, .v = .{ .string = "Alice" }, .tx = 100, .op = true });
    _ = try mgr.insertDatom(.{ .e = e2, .a = USER_ATTR_NAME, .v = .{ .string = "Bob" }, .tx = 100, .op = true });
    _ = try mgr.insertDatom(.{ .e = e1, .a = USER_ATTR_EMAIL, .v = .{ .string = "alice@example.com" }, .tx = 100, .op = true });
    _ = try mgr.insertDatom(.{ .e = e1, .a = USER_ATTR_FRIEND, .v = .{ .ref = e2 }, .tx = 100, .op = true });
    _ = try mgr.insertDatom(.{ .e = e2, .a = USER_ATTR_FRIEND, .v = .{ .ref = e3 }, .tx = 100, .op = true });

    // TxLog should have all 5 datoms
    var txlog_count: u32 = 0;
    var it = mgr.txlog.seekFirst();
    while (it.next()) |_| txlog_count += 1;
    try testing.expectEqual(@as(u32, 5), txlog_count);

    // AVE should have email + 2 friend datoms = 3
    var ave_count: u32 = 0;
    var ait = mgr.ave.seekFirst();
    while (ait.next()) |_| ave_count += 1;
    try testing.expectEqual(@as(u32, 3), ave_count);

    // VAE should have 2 friend datoms
    var vae_count: u32 = 0;
    var vit = mgr.vae.seekFirst();
    while (vit.next()) |_| vae_count += 1;
    try testing.expectEqual(@as(u32, 2), vae_count);
}

test "insertDatom returns updated roots" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var fm = try file.FileManager.open(tmp.dir, "test.zat", .{});
    defer fm.close();

    const setup = try setupTestDb(&fm);
    var mgr = IndexManager.init(
        .{ .eav = setup.eav_root },
        &fm,
        setup.cache,
    );

    const roots_before = mgr.currentRoots();

    const e1 = userEntity(1);
    const roots_after = try mgr.insertDatom(.{
        .e = e1,
        .a = USER_ATTR_FRIEND,
        .v = .{ .ref = userEntity(2) },
        .tx = 100,
        .op = true,
    });

    // EAV root should change (new data inserted)
    try testing.expect(roots_after.eav != roots_before.eav);
    // AVE should be non-zero (friend is indexed)
    try testing.expect(roots_after.ave != 0);
    // VAE should be non-zero (friend is ref)
    try testing.expect(roots_after.vae != 0);
    // TxLog should be non-zero
    try testing.expect(roots_after.txlog != 0);

    // currentRoots should match returned roots
    const current = mgr.currentRoots();
    try testing.expectEqual(roots_after.eav, current.eav);
    try testing.expectEqual(roots_after.ave, current.ave);
    try testing.expectEqual(roots_after.vae, current.vae);
    try testing.expectEqual(roots_after.txlog, current.txlog);
}
