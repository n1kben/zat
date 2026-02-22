// ZatDB — Transaction Processor
//
// Write coordinator that ties together all layers. Accepts user operations,
// validates against schema, resolves tempids, enforces constraints, generates
// datoms, writes to indexes, and commits via dual meta pages.
//
// Pipeline: validate → resolve tempids → upsert → cardinality/unique checks
//           → generate datoms → write indexes → commit → reload schema

const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;
const encoding = @import("encoding.zig");
const schema = @import("schema.zig");
const index = @import("index.zig");
const btree = @import("btree.zig");
const file = @import("file.zig");
const meta = @import("meta.zig");
const freelist = @import("freelist.zig");
const page = @import("page.zig");

// ============================================================================
// Types
// ============================================================================

pub const OpType = enum { db_add, db_retract };

pub const EntityRef = union(enum) {
    id: u64,
    tempid: []const u8,
    db_tx: void,
};

pub const TxOp = struct {
    op: OpType,
    entity: EntityRef,
    attr: []const u8,
    value: encoding.Value,
};

const MAX_TEMPIDS = 64;
const MAX_TEMPID_LEN = 64;

pub const TempidMap = struct {
    names: [MAX_TEMPIDS][MAX_TEMPID_LEN]u8 = undefined,
    name_lens: [MAX_TEMPIDS]u8 = .{0} ** MAX_TEMPIDS,
    ids: [MAX_TEMPIDS]u64 = .{0} ** MAX_TEMPIDS,
    count: u8 = 0,

    pub fn resolve(self: *const TempidMap, name: []const u8) ?u64 {
        for (0..self.count) |i| {
            if (mem.eql(u8, self.names[i][0..self.name_lens[i]], name))
                return self.ids[i];
        }
        return null;
    }

    fn put(self: *TempidMap, name: []const u8, id: u64) void {
        if (self.count >= MAX_TEMPIDS) return;
        const len: u8 = @intCast(@min(name.len, MAX_TEMPID_LEN));
        @memcpy(self.names[self.count][0..len], name[0..len]);
        self.name_lens[self.count] = len;
        self.ids[self.count] = id;
        self.count += 1;
    }

    fn remap(self: *TempidMap, name: []const u8, new_id: u64) void {
        for (0..self.count) |i| {
            if (mem.eql(u8, self.names[i][0..self.name_lens[i]], name)) {
                self.ids[i] = new_id;
                return;
            }
        }
    }
};

pub const TxReport = struct {
    tx_id: u64,
    tempids: TempidMap,
    datom_count: u32,
};

const MAX_DATOMS = 512;

pub const TxError = error{
    UnknownAttribute,
    TypeMismatch,
    UniqueValueConflict,
    TempidOverflow,
    DatomOverflow,
};

// ============================================================================
// Database
// ============================================================================

pub const Database = struct {
    fm: file.FileManager,
    schema_cache: schema.SchemaCache,
    roots: index.IndexRoots,
    free_root: u64,
    carry_forward: freelist.FreePageTracker,
    tx_id: u64,
    next_entity_id: u64,
    datom_count: u64,

    pub fn open(dir: std.fs.Dir, name: []const u8) !Database {
        var fm = try file.FileManager.open(dir, name, .{});
        errdefer fm.close();

        var active = try fm.readActiveMeta();

        if (active.eav_root_page == 0) {
            // New database — bootstrap schema
            const boot = try schema.bootstrap(&fm);
            active.eav_root_page = boot.eav_root;
            active.next_entity_id = boot.next_entity_id;
            active.next_page = fm.next_page;
            active.page_size = fm.page_size;

            try fm.sync();
            try fm.commitMeta(active);
            try fm.sync();
            try fm.remap();
        }

        // Load schema cache from EAV tree
        const eav_tree = btree.BPlusTree.init(active.eav_root_page, &fm, schema.eavKeyCmp);
        const cache = schema.SchemaCache.load(eav_tree);

        return .{
            .fm = fm,
            .schema_cache = cache,
            .roots = .{
                .eav = active.eav_root_page,
                .ave = active.ave_root_page,
                .vae = active.vae_root_page,
                .txlog = active.txlog_root_page,
            },
            .free_root = active.free_root_page,
            .carry_forward = .{},
            .tx_id = active.tx_id,
            .next_entity_id = active.next_entity_id,
            .datom_count = active.datom_count,
        };
    }

    pub fn close(self: *Database) void {
        self.fm.close();
    }

    // ====================================================================
    // Transact
    // ====================================================================

    pub fn transact(self: *Database, ops: []const TxOp) !TxReport {
        if (ops.len == 0) return error.Overflow;
        if (ops.len > 256) return error.Overflow;

        const new_tx_id = self.tx_id + 1;
        const tx_entity = schema.makeEntityId(schema.PARTITION_TX, @intCast(new_tx_id));

        // --- 1. Validate: resolve attr keywords, type-check values ---
        var resolved_attrs: [256]u64 = undefined;
        for (ops, 0..) |op, i| {
            const attr_eid = self.schema_cache.resolveIdent(op.attr) orelse
                return error.UnknownAttribute;
            resolved_attrs[i] = attr_eid;

            if (!self.schema_cache.validateType(attr_eid, op.value))
                return error.TypeMismatch;
        }

        // --- 2. Resolve tempids ---
        var next_eid = self.next_entity_id;
        var tempids = TempidMap{};
        for (ops, 0..) |op, i| {
            switch (op.entity) {
                .tempid => |name| {
                    if (tempids.resolve(name) == null) {
                        if (tempids.count >= MAX_TEMPIDS)
                            return error.TempidOverflow;
                        const partition = determineTempidPartition(name, ops, resolved_attrs[0..ops.len]);
                        const eid = schema.makeEntityId(partition, @intCast(next_eid));
                        next_eid += 1;
                        tempids.put(name, eid);
                    }
                },
                else => {},
            }
            _ = i;
        }

        // --- 3. Unique/identity upsert ---
        // Reconstruct IndexManager with current roots
        var idx_mgr = index.IndexManager.init(self.roots, &self.fm, self.schema_cache);

        for (ops, 0..) |op, i| {
            if (op.op != .db_add) continue;
            const attr_eid = resolved_attrs[i];
            const attr = self.schema_cache.getAttr(attr_eid) orelse continue;
            if (attr.unique != .identity) continue;

            switch (op.entity) {
                .tempid => |name| {
                    if (lookupAveEntity(&idx_mgr, attr_eid, op.value)) |existing_eid| {
                        tempids.remap(name, existing_eid);
                    }
                },
                else => {},
            }
        }

        // --- 4. Wire FreePageTracker to all trees ---
        var free_mgr = freelist.FreePageManager.init(self.free_root, &self.fm);
        free_mgr.carry_forward = self.carry_forward;

        idx_mgr.eav.freed = &free_mgr.tracker;
        idx_mgr.ave.freed = &free_mgr.tracker;
        idx_mgr.vae.freed = &free_mgr.tracker;
        idx_mgr.txlog.freed = &free_mgr.tracker;

        // --- 5. Process ops: generate and write datoms ---
        var datom_count: u32 = 0;

        for (ops, 0..) |op, i| {
            const attr_eid = resolved_attrs[i];
            const entity_id = resolveEntity(op.entity, &tempids, tx_entity);

            if (op.op == .db_add) {
                const attr = self.schema_cache.getAttr(attr_eid);

                // Cardinality/one: implicit retraction of old value
                if (attr) |a| {
                    if (a.cardinality == .one) {
                        const old_val = findExistingValue(&idx_mgr, entity_id, attr_eid);
                        if (old_val) |ov| {
                            if (!ov.eql(op.value)) {
                                // Generate implicit retraction
                                try deleteDatomFromIndexes(&idx_mgr, entity_id, attr_eid, ov);
                                try insertTxLogRetraction(&idx_mgr, new_tx_id, entity_id, attr_eid, ov);
                                datom_count += 1;
                            } else {
                                // Idempotent — same value, skip
                                continue;
                            }
                        }
                    }
                }

                // Unique/value check
                if (attr) |a| {
                    if (a.unique == .value) {
                        if (lookupAveEntity(&idx_mgr, attr_eid, op.value)) |existing_eid| {
                            if (existing_eid != entity_id) {
                                return error.UniqueValueConflict;
                            }
                        }
                    }
                }

                // Insert assertion datom
                _ = try idx_mgr.insertDatom(.{
                    .e = entity_id,
                    .a = attr_eid,
                    .v = op.value,
                    .tx = new_tx_id,
                    .op = true,
                });
                datom_count += 1;
            } else {
                // db_retract
                try deleteDatomFromIndexes(&idx_mgr, entity_id, attr_eid, op.value);
                try insertTxLogRetraction(&idx_mgr, new_tx_id, entity_id, attr_eid, op.value);
                datom_count += 1;
            }
        }

        // --- 6. Add tx entity datom (:db/txInstant) ---
        _ = try idx_mgr.insertDatom(.{
            .e = tx_entity,
            .a = schema.ATTR_TX_INSTANT,
            .v = .{ .instant = std.time.microTimestamp() },
            .tx = new_tx_id,
            .op = true,
        });
        datom_count += 1;

        // --- 7. Unwire tracker, commit freed pages ---
        idx_mgr.eav.freed = null;
        idx_mgr.ave.freed = null;
        idx_mgr.vae.freed = null;
        idx_mgr.txlog.freed = null;

        try free_mgr.commitFreedPages(new_tx_id);

        // --- 8. Commit: sync → meta → sync → remap ---
        try self.fm.sync();

        const new_roots = idx_mgr.currentRoots();
        try self.fm.commitMeta(.{
            .page_size = self.fm.page_size,
            .tx_id = new_tx_id,
            .eav_root_page = new_roots.eav,
            .ave_root_page = new_roots.ave,
            .vae_root_page = new_roots.vae,
            .txlog_root_page = new_roots.txlog,
            .free_root_page = free_mgr.freeRoot(),
            .next_entity_id = next_eid,
            .next_page = self.fm.next_page,
            .datom_count = self.datom_count + datom_count,
        });

        try self.fm.sync();
        try self.fm.remap();

        // --- 9. Update Database state ---
        self.tx_id = new_tx_id;
        self.next_entity_id = next_eid;
        self.datom_count += datom_count;
        self.roots = new_roots;
        self.free_root = free_mgr.freeRoot();
        self.carry_forward = free_mgr.carry_forward;

        // --- 10. Reload schema if any partition-0 entity was touched ---
        var schema_changed = false;
        for (ops, 0..) |op, oi| {
            _ = oi;
            const eid = resolveEntity(op.entity, &tempids, tx_entity);
            if (schema.partitionOf(eid) == schema.PARTITION_DB) {
                schema_changed = true;
                break;
            }
        }
        if (schema_changed) {
            const eav_tree = btree.BPlusTree.init(self.roots.eav, &self.fm, schema.eavKeyCmp);
            self.schema_cache = schema.SchemaCache.load(eav_tree);
        }

        return .{
            .tx_id = new_tx_id,
            .tempids = tempids,
            .datom_count = datom_count,
        };
    }
};

// ============================================================================
// Internal helpers
// ============================================================================

fn resolveEntity(entity: EntityRef, tempids: *const TempidMap, tx_entity: u64) u64 {
    return switch (entity) {
        .id => |id| id,
        .tempid => |name| tempids.resolve(name) orelse 0,
        .db_tx => tx_entity,
    };
}

fn determineTempidPartition(name: []const u8, ops: []const TxOp, resolved_attrs: []const u64) u10 {
    for (ops, 0..) |op, i| {
        switch (op.entity) {
            .tempid => |n| {
                if (mem.eql(u8, n, name) and isBootstrapAttr(resolved_attrs[i])) {
                    return schema.PARTITION_DB;
                }
            },
            else => {},
        }
    }
    return schema.PARTITION_USER;
}

fn isBootstrapAttr(attr_eid: u64) bool {
    return attr_eid >= 1 and attr_eid <= 8;
}

fn lookupAveEntity(idx_mgr: *index.IndexManager, attr_eid: u64, value: encoding.Value) ?u64 {
    if (idx_mgr.ave.root == 0) return null;

    var prefix_buf: [512]u8 = undefined;
    encoding.writeU64(prefix_buf[0..8], attr_eid);
    const val_len = encoding.encode(value, prefix_buf[8..]);
    const prefix_len = 8 + val_len;

    var it = idx_mgr.ave.seek(prefix_buf[0..prefix_len]);
    const entry = it.next() orelse return null;

    // Verify attr matches
    if (index.decodeAveAttr(entry.key) != attr_eid) return null;

    // Verify value matches
    const entry_value = index.decodeAveValue(entry.key);
    if (!entry_value.eql(value)) return null;

    return index.decodeAveEntity(entry.key);
}

fn findExistingValue(idx_mgr: *index.IndexManager, entity_id: u64, attr_eid: u64) ?encoding.Value {
    if (idx_mgr.eav.root == 0) return null;

    var prefix: [16]u8 = undefined;
    encoding.writeU64(prefix[0..8], entity_id);
    encoding.writeU64(prefix[8..16], attr_eid);

    var it = idx_mgr.eav.seek(&prefix);
    const entry = it.next() orelse return null;

    if (schema.decodeEavEntity(entry.key) != entity_id) return null;
    if (schema.decodeEavAttr(entry.key) != attr_eid) return null;

    return schema.decodeEavValue(entry.key);
}

fn deleteDatomFromIndexes(idx_mgr: *index.IndexManager, entity_id: u64, attr_eid: u64, value: encoding.Value) !void {
    var key_buf: [512]u8 = undefined;

    // Delete from EAV
    const eav_len = schema.encodeEavKey(&key_buf, entity_id, attr_eid, value);
    try idx_mgr.eav.delete(key_buf[0..eav_len]);

    // Delete from AVE if indexed
    if (idx_mgr.schema_cache.isIndexed(attr_eid)) {
        const ave_len = index.encodeAveKey(&key_buf, attr_eid, value, entity_id);
        try idx_mgr.ave.delete(key_buf[0..ave_len]);
    }

    // Delete from VAE if ref
    if (idx_mgr.schema_cache.isRef(attr_eid)) {
        switch (value) {
            .ref => |ref_val| {
                const vae_len = index.encodeVaeKey(&key_buf, ref_val, attr_eid, entity_id);
                try idx_mgr.vae.delete(key_buf[0..vae_len]);
            },
            else => {},
        }
    }
}

fn insertTxLogRetraction(idx_mgr: *index.IndexManager, tx_id: u64, entity_id: u64, attr_eid: u64, value: encoding.Value) !void {
    var key_buf: [512]u8 = undefined;
    const txlog_len = index.encodeTxLogKey(&key_buf, tx_id, entity_id, attr_eid, value, false);
    try idx_mgr.txlog.insert(key_buf[0..txlog_len], &.{});
}

// ============================================================================
// Tests
// ============================================================================

fn userEntity(seq: u54) u64 {
    return schema.makeEntityId(schema.PARTITION_USER, seq);
}

fn txEntity(tx_id: u64) u64 {
    return schema.makeEntityId(schema.PARTITION_TX, @intCast(tx_id));
}

// --- Subtask 1: Database Open/Close ---

test "open creates db with bootstrap schema" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    try testing.expectEqual(@as(u64, 0), db.tx_id);
    try testing.expectEqual(@as(u64, 9), db.next_entity_id);
    try testing.expectEqual(@as(u16, 8), db.schema_cache.count);

    // Verify bootstrap attrs are resolvable
    try testing.expect(db.schema_cache.resolveIdent(":db/ident") != null);
    try testing.expect(db.schema_cache.resolveIdent(":db/valueType") != null);
    try testing.expect(db.schema_cache.resolveIdent(":db/cardinality") != null);
    try testing.expect(db.schema_cache.resolveIdent(":db/txInstant") != null);
}

test "reopen recovers state" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var db = try Database.open(tmp.dir, "test.zat");
        db.close();
    }

    {
        var db = try Database.open(tmp.dir, "test.zat");
        defer db.close();

        try testing.expectEqual(@as(u64, 0), db.tx_id);
        try testing.expectEqual(@as(u64, 9), db.next_entity_id);
        try testing.expectEqual(@as(u16, 8), db.schema_cache.count);
    }
}

// --- Subtask 2: Basic Assert — Single Datom ---

test "basic assert with known entity" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install :user/name schema attr
    const report1 = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "user/name" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "user/name" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "user/name" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });
    try testing.expectEqual(@as(u64, 1), report1.tx_id);

    // Get the attr entity ID
    const attr_eid = db.schema_cache.resolveIdent(":user/name").?;

    // Assert a datom with a known entity ID
    const e1 = userEntity(100);
    const report2 = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });
    try testing.expectEqual(@as(u64, 2), report2.tx_id);

    // Verify in EAV
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;
    const kl = schema.encodeEavKey(&key_buf, e1, attr_eid, .{ .string = "Alice" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
}

// --- Subtask 3: Schema Attribute Installation ---

test "schema attr installation updates cache" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Before: :user/name not in cache
    try testing.expect(db.schema_cache.resolveIdent(":user/name") == null);

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    // After: :user/name in cache
    const eid = db.schema_cache.resolveIdent(":user/name");
    try testing.expect(eid != null);
    try testing.expect(db.schema_cache.validateType(eid.?, .{ .string = "test" }));

    // Usable in next tx
    const e1 = userEntity(1);
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });

    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;
    const kl = schema.encodeEavKey(&key_buf, e1, eid.?, .{ .string = "Alice" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
}

// --- Subtask 4: Schema Validation ---

test "unknown attribute returns error" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    const tx_before = db.tx_id;
    const result = db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = userEntity(1) }, .attr = ":nonexistent/attr", .value = .{ .string = "x" } },
    });
    try testing.expectError(error.UnknownAttribute, result);
    try testing.expectEqual(tx_before, db.tx_id);
}

test "type mismatch returns error" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install :user/name as string type
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const tx_before = db.tx_id;
    // Try to assert an i64 value on a string attr
    const result = db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = userEntity(1) }, .attr = ":user/name", .value = .{ .i64 = 42 } },
    });
    try testing.expectError(error.TypeMismatch, result);
    try testing.expectEqual(tx_before, db.tx_id);
}

// --- Subtask 5: Tempid Resolution ---

test "tempid resolution: same tempid gets same entity" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install two string attrs
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
        .{ .op = .db_add, .entity = .{ .tempid = "b" }, .attr = ":db/ident", .value = .{ .keyword = ":user/email" } },
        .{ .op = .db_add, .entity = .{ .tempid = "b" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "b" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const name_eid = db.schema_cache.resolveIdent(":user/name").?;
    const email_eid = db.schema_cache.resolveIdent(":user/email").?;

    // Use same tempid for two ops
    const report = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "alice" }, .attr = ":user/name", .value = .{ .string = "Alice" } },
        .{ .op = .db_add, .entity = .{ .tempid = "alice" }, .attr = ":user/email", .value = .{ .string = "alice@test.com" } },
    });

    const alice_eid = report.tempids.resolve("alice").?;
    try testing.expectEqual(schema.PARTITION_USER, schema.partitionOf(alice_eid));

    // Both datoms should be on the same entity
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;
    var kl = schema.encodeEavKey(&key_buf, alice_eid, name_eid, .{ .string = "Alice" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
    kl = schema.encodeEavKey(&key_buf, alice_eid, email_eid, .{ .string = "alice@test.com" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
}

test "tempid resolution: different tempids get different entities" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const report = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "alice" }, .attr = ":user/name", .value = .{ .string = "Alice" } },
        .{ .op = .db_add, .entity = .{ .tempid = "bob" }, .attr = ":user/name", .value = .{ .string = "Bob" } },
    });

    const alice_eid = report.tempids.resolve("alice").?;
    const bob_eid = report.tempids.resolve("bob").?;
    try testing.expect(alice_eid != bob_eid);
}

// --- Subtask 6: Transaction Entity (txInstant) ---

test "tx entity has txInstant datom" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const report = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = userEntity(1) }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });

    // Tx entity should have :db/txInstant in EAV
    const tx_eid = txEntity(report.tx_id);
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);

    // Seek for tx entity's attrs
    var prefix: [16]u8 = undefined;
    encoding.writeU64(prefix[0..8], tx_eid);
    encoding.writeU64(prefix[8..16], schema.ATTR_TX_INSTANT);

    var it = eav_tree.seek(&prefix);
    const entry = it.next() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(tx_eid, schema.decodeEavEntity(entry.key));
    try testing.expectEqual(@as(u64, schema.ATTR_TX_INSTANT), schema.decodeEavAttr(entry.key));

    // Verify all datoms appear in TxLog under same tx_id
    const txlog_tree = btree.BPlusTree.init(db.roots.txlog, &db.fm, index.txLogKeyCmp);
    var tx_prefix: [8]u8 = undefined;
    encoding.writeU64(&tx_prefix, report.tx_id);

    var txit = txlog_tree.seek(&tx_prefix);
    var txlog_count: u32 = 0;
    while (txit.next()) |txe| {
        if (index.decodeTxLogTx(txe.key) != report.tx_id) break;
        txlog_count += 1;
    }
    try testing.expectEqual(report.datom_count, txlog_count);
}

// --- Subtask 7: Cardinality/One Enforcement ---

test "cardinality one: second value replaces first" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install :user/name (card/one)
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const name_eid = db.schema_cache.resolveIdent(":user/name").?;
    const e1 = userEntity(1);

    // Assert "Alice"
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });

    // Assert "Bob" on same entity+attr → should replace
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Bob" } },
    });

    // EAV should only have "Bob", not "Alice"
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;

    var kl = schema.encodeEavKey(&key_buf, e1, name_eid, .{ .string = "Alice" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) == null);

    kl = schema.encodeEavKey(&key_buf, e1, name_eid, .{ .string = "Bob" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
}

test "cardinality one: same value is idempotent" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const e1 = userEntity(1);

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });

    const datoms_before = db.datom_count;

    // Assert same value again — idempotent, only txInstant datom added
    const report = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });

    // Only the txInstant datom should have been written
    try testing.expectEqual(@as(u32, 1), report.datom_count);
    try testing.expectEqual(datoms_before + 1, db.datom_count);
}

test "cardinality many: allows multiple values" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install :user/tag (card/many)
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/tag" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/many" } },
    });

    const tag_eid = db.schema_cache.resolveIdent(":user/tag").?;
    const e1 = userEntity(1);

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/tag", .value = .{ .string = "admin" } },
    });
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/tag", .value = .{ .string = "staff" } },
    });

    // Both values should be in EAV
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;

    var kl = schema.encodeEavKey(&key_buf, e1, tag_eid, .{ .string = "admin" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);

    kl = schema.encodeEavKey(&key_buf, e1, tag_eid, .{ .string = "staff" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
}

// --- Subtask 8: Retraction Support ---

test "retraction removes from EAV" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const name_eid = db.schema_cache.resolveIdent(":user/name").?;
    const e1 = userEntity(1);

    // Assert
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });

    // Retract
    const report = try db.transact(&[_]TxOp{
        .{ .op = .db_retract, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });

    // Not in EAV
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;
    const kl = schema.encodeEavKey(&key_buf, e1, name_eid, .{ .string = "Alice" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) == null);

    // Retraction recorded in TxLog with op=false
    const txlog_tree = btree.BPlusTree.init(db.roots.txlog, &db.fm, index.txLogKeyCmp);
    var txlog_buf: [512]u8 = undefined;
    const txlog_kl = index.encodeTxLogKey(&txlog_buf, report.tx_id, e1, name_eid, .{ .string = "Alice" }, false);
    try testing.expect(txlog_tree.lookup(txlog_buf[0..txlog_kl]) != null);
}

// --- Subtask 9: Unique Value Constraint ---

test "unique value: different entity same value returns error" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install :user/email with unique/value
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/email" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/unique", .value = .{ .keyword = ":db.unique/value" } },
    });

    const e1 = userEntity(1);
    const e2 = userEntity(2);

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/email", .value = .{ .string = "alice@test.com" } },
    });

    // Same value on different entity → error
    const result = db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e2 }, .attr = ":user/email", .value = .{ .string = "alice@test.com" } },
    });
    try testing.expectError(error.UniqueValueConflict, result);
}

test "unique value: same entity same value is OK" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/email" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/unique", .value = .{ .keyword = ":db.unique/value" } },
    });

    const e1 = userEntity(1);
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/email", .value = .{ .string = "alice@test.com" } },
    });

    // Same entity, same value → OK (idempotent)
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/email", .value = .{ .string = "alice@test.com" } },
    });
}

// --- Subtask 10: Unique Identity Upsert ---

test "unique identity: upserts tempid to existing entity" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install :user/email with unique/identity and :user/name
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "e" }, .attr = ":db/ident", .value = .{ .keyword = ":user/email" } },
        .{ .op = .db_add, .entity = .{ .tempid = "e" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "e" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
        .{ .op = .db_add, .entity = .{ .tempid = "e" }, .attr = ":db/unique", .value = .{ .keyword = ":db.unique/identity" } },
        .{ .op = .db_add, .entity = .{ .tempid = "n" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "n" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "n" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const name_eid = db.schema_cache.resolveIdent(":user/name").?;

    // First tx: create entity with email
    const report1 = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "alice" }, .attr = ":user/email", .value = .{ .string = "alice@test.com" } },
        .{ .op = .db_add, .entity = .{ .tempid = "alice" }, .attr = ":user/name", .value = .{ .string = "Alice" } },
    });
    const alice_eid = report1.tempids.resolve("alice").?;

    // Second tx: different tempid, same email → should upsert to same entity
    const report2 = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "person" }, .attr = ":user/email", .value = .{ .string = "alice@test.com" } },
        .{ .op = .db_add, .entity = .{ .tempid = "person" }, .attr = ":user/name", .value = .{ .string = "Alice Updated" } },
    });
    const person_eid = report2.tempids.resolve("person").?;

    // Same entity
    try testing.expectEqual(alice_eid, person_eid);

    // Name should be updated (card/one replacement)
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;

    // Old name should be gone
    var kl = schema.encodeEavKey(&key_buf, alice_eid, name_eid, .{ .string = "Alice" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) == null);

    // New name should be present
    kl = schema.encodeEavKey(&key_buf, alice_eid, name_eid, .{ .string = "Alice Updated" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
}

// --- Subtask 11: Crash Recovery ---

test "crash recovery: reopen recovers data" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const e1 = userEntity(1);

    {
        var db = try Database.open(tmp.dir, "test.zat");
        defer db.close();

        _ = try db.transact(&[_]TxOp{
            .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
            .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
            .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
        });

        _ = try db.transact(&[_]TxOp{
            .{ .op = .db_add, .entity = .{ .id = e1 }, .attr = ":user/name", .value = .{ .string = "Alice" } },
        });
    }

    // Reopen
    {
        var db = try Database.open(tmp.dir, "test.zat");
        defer db.close();

        try testing.expectEqual(@as(u64, 2), db.tx_id);
        // next_entity_id should be restored
        try testing.expect(db.next_entity_id > 9);

        // Schema should be loaded
        const name_eid = db.schema_cache.resolveIdent(":user/name");
        try testing.expect(name_eid != null);

        // Data should be present in EAV
        const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
        var key_buf: [512]u8 = undefined;
        const kl = schema.encodeEavKey(&key_buf, e1, name_eid.?, .{ .string = "Alice" });
        try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
    }
}

// --- Subtask 12: Multi-Datom Transactions & db_tx ---

test "multi-datom tx: all get same tx_id in TxLog" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const report = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "alice" }, .attr = ":user/name", .value = .{ .string = "Alice" } },
        .{ .op = .db_add, .entity = .{ .tempid = "bob" }, .attr = ":user/name", .value = .{ .string = "Bob" } },
        .{ .op = .db_add, .entity = .{ .tempid = "charlie" }, .attr = ":user/name", .value = .{ .string = "Charlie" } },
    });

    // All datoms + txInstant should be under same tx_id
    const txlog_tree = btree.BPlusTree.init(db.roots.txlog, &db.fm, index.txLogKeyCmp);
    var tx_prefix: [8]u8 = undefined;
    encoding.writeU64(&tx_prefix, report.tx_id);

    var it = txlog_tree.seek(&tx_prefix);
    var count: u32 = 0;
    while (it.next()) |entry| {
        if (index.decodeTxLogTx(entry.key) != report.tx_id) break;
        count += 1;
    }
    // 3 user datoms + 1 txInstant = 4
    try testing.expectEqual(@as(u32, 4), count);
    try testing.expectEqual(@as(u32, 4), report.datom_count);
}

test "db_tx entity ref asserts on tx entity" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.open(tmp.dir, "test.zat");
    defer db.close();

    // Install :tx/note (string attr for tx metadata)
    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":tx/note" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const note_eid = db.schema_cache.resolveIdent(":tx/note").?;

    _ = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/ident", .value = .{ .keyword = ":user/name" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/valueType", .value = .{ .keyword = ":db.type/string" } },
        .{ .op = .db_add, .entity = .{ .tempid = "a" }, .attr = ":db/cardinality", .value = .{ .keyword = ":db.cardinality/one" } },
    });

    const report = try db.transact(&[_]TxOp{
        .{ .op = .db_add, .entity = .{ .id = userEntity(1) }, .attr = ":user/name", .value = .{ .string = "Alice" } },
        .{ .op = .db_add, .entity = .{ .db_tx = {} }, .attr = ":tx/note", .value = .{ .string = "imported data" } },
    });

    // :tx/note should be on the tx entity
    const tx_eid = txEntity(report.tx_id);
    const eav_tree = btree.BPlusTree.init(db.roots.eav, &db.fm, schema.eavKeyCmp);
    var key_buf: [512]u8 = undefined;
    const kl = schema.encodeEavKey(&key_buf, tx_eid, note_eid, .{ .string = "imported data" });
    try testing.expect(eav_tree.lookup(key_buf[0..kl]) != null);
}
