const std = @import("std");

const Allocator = std.mem.Allocator;

pub const Config = struct {
    const Self = @This();

    dbfilename: ?[]const u8 = null,
    dir: ?[]const u8 = null,
    port: ?u16 = null,
    replicaof: ?[]const u8 = null,

    pub fn deinit(self: Self, allocator: Allocator) void {
        if (self.dir) |dir| {
            allocator.free(dir);
        }

        if (self.dbfilename) |dbfilename| {
            allocator.free(dbfilename);
        }

        if (self.replicaof) |replicaof| {
            allocator.free(replicaof);
        }
    }
};

fn expectArg(iter: *std.process.ArgIterator) ![]const u8 {
    if (iter.next()) |arg| {
        return arg;
    }

    return error.MissingArgument;
}

pub fn process(allocator: Allocator) !Config {
    var iter = std.process.args();
    var config = Config{};

    _ = iter.next();

    while (iter.next()) |arg| {
        if (std.mem.eql(u8, "--dir", arg)) {
            const dir = try expectArg(&iter);

            config.dir = try allocator.dupe(u8, dir);
        } else if (std.mem.eql(u8, "--dbfilename", arg)) {
            const dbfilename = try expectArg(&iter);

            config.dbfilename = try allocator.dupe(u8, dbfilename);
        } else if (std.mem.eql(u8, "--port", arg)) {
            const port_str = try expectArg(&iter);

            config.port = try std.fmt.parseUnsigned(u16, port_str, 10);
        } else if (std.mem.eql(u8, "--replicaof", arg)) {
            const replicaof = try expectArg(&iter);

            config.replicaof = try allocator.dupe(u8, replicaof);
        } else {
            return error.UnexpectedArgument;
        }
    }

    return config;
}
