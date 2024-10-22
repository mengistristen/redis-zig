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

fn expectArg(comptime T: type, iter: *T) ![]const u8 {
    if (iter.next()) |arg| {
        return arg;
    }

    return error.MissingArgument;
}

pub fn process(comptime T: type, iter: *T, allocator: Allocator) !Config {
    var config = Config{};

    _ = iter.next();

    while (iter.next()) |arg| {
        if (std.mem.eql(u8, "--dir", arg)) {
            const dir = try expectArg(T, iter);

            if (config.dir) |old_dir| {
                allocator.free(old_dir);
            }

            config.dir = try allocator.dupe(u8, dir);
        } else if (std.mem.eql(u8, "--dbfilename", arg)) {
            const dbfilename = try expectArg(T, iter);

            if (config.dbfilename) |old_dbfilename| {
                allocator.free(old_dbfilename);
            }

            config.dbfilename = try allocator.dupe(u8, dbfilename);
        } else if (std.mem.eql(u8, "--replicaof", arg)) {
            const replicaof = try expectArg(T, iter);

            if (config.replicaof) |old_replicaof| {
                allocator.free(old_replicaof);
            }

            config.replicaof = try allocator.dupe(u8, replicaof);
        } else if (std.mem.eql(u8, "--port", arg)) {
            const port_str = try expectArg(T, iter);

            config.port = try std.fmt.parseUnsigned(u16, port_str, 10);
        } else {
            return error.UnexpectedArgument;
        }
    }

    return config;
}

const testing = struct {
    fn ArgsIterator(size: usize) type {
        return struct {
            const Self = @This();

            slice: [size][]const u8,
            index: usize = 0,

            pub fn next(self: *Self) ?[]const u8 {
                if (self.index >= self.slice.len) return null;

                const result = self.slice[self.index];

                self.index += 1;

                return result;
            }
        };
    }
};

test "duplicate arguments" {
    const args = [_][]const u8{ "command", "--dbfilename", "asdf", "--dbfilename", "fdsa" };
    var iter = testing.ArgsIterator(args.len){
        .slice = args,
    };

    const config = try process(@TypeOf(iter), &iter, std.testing.allocator);
    defer config.deinit(std.testing.allocator);
}
