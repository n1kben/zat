// ZatDB — Datomic-style embedded database in Zig
//
// Module imports will be added as implementation progresses:
pub const encoding = @import("encoding.zig");
pub const page = @import("page.zig");
pub const meta = @import("meta.zig");
pub const file = @import("file.zig");
pub const btree = @import("btree.zig");
// const index = @import("index.zig");
// const tx = @import("tx.zig");
// const schema = @import("schema.zig");

const std = @import("std");

test {
    _ = encoding;
    _ = page;
    _ = meta;
    _ = file;
    _ = btree;
}
