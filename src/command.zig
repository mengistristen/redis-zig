const std = @import("std");
const net = std.net;

const Allocator = std.mem.Allocator;

const datastore = @import("datastore.zig");
const resp = @import("resp.zig");

fn expectArg(iter: *resp.ValueIterator) !resp.BulkString {
    if (iter.next()) |value| {
        return value.unwrapBulkString();
    } else {
        return error.MissingArgument;
    }
}

fn acceptArg(iter: *resp.ValueIterator) !?resp.BulkString {
    if (iter.next()) |value| {
        return try value.unwrapBulkString();
    } else {
        return null;
    }
}

pub fn handle(
    ctx: anytype,
    value: resp.Value,
) !void {
    const args = (try value.unwrapArray()).data;
    var iter = resp.ValueIterator{
        .values = args,
    };

    const command = (try expectArg(&iter)).data;

    if (std.ascii.eqlIgnoreCase("ping", command)) {
        _ = try ctx.connection.stream.write("+PONG\r\n");
    } else if (std.ascii.eqlIgnoreCase("echo", command)) {
        try handleEcho(ctx, &iter);
    } else if (std.ascii.eqlIgnoreCase("get", command)) {
        try handleGet(ctx, &iter);
    } else if (std.ascii.eqlIgnoreCase("set", command)) {
        try handleSet(ctx, &iter);
    } else if (std.ascii.eqlIgnoreCase("config", command)) {
        try handleConfig(ctx, &iter);
    } else if (std.ascii.eqlIgnoreCase("info", command)) {
        try handleInfo(ctx, &iter);
    } else {
        return error.UnknownCommand;
    }
}

fn handleEcho(ctx: anytype, iter: *resp.ValueIterator) !void {
    const paramRaw = (try expectArg(iter)).raw;

    _ = try ctx.connection.stream.write(paramRaw);
}

fn handleGet(ctx: anytype, iter: *resp.ValueIterator) !void {
    const key = (try expectArg(iter)).data;

    if (datastore.get(@TypeOf(ctx.datastore.*), ctx.datastore, key)) |data| {
        const formatted = try std.fmt.allocPrint(ctx.allocator, "${d}\r\n{s}\r\n", .{ data.len, data });
        defer ctx.allocator.free(formatted);

        _ = try ctx.connection.stream.write(formatted);
    } else {
        _ = try ctx.connection.stream.write("$-1\r\n");
    }
}

fn handleSet(ctx: anytype, iter: *resp.ValueIterator) !void {
    const k = (try expectArg(iter)).data;
    const v = (try expectArg(iter)).data;

    var expiry: ?i64 = null;

    if (try acceptArg(iter)) |arg| {
        if (std.ascii.eqlIgnoreCase("px", arg.data)) {
            const amount_str = (try expectArg(iter)).data;

            const amount = try std.fmt.parseInt(i64, amount_str, 10);

            expiry = amount;
        }
    }

    if (datastore.set(@TypeOf(ctx.datastore.*), ctx.datastore, k, v, expiry)) |_| {
        _ = try ctx.connection.stream.write("+OK\r\n");
    } else |_| {
        _ = try ctx.connection.stream.write("-error setting value\r\n");
    }
}

fn handleConfig(ctx: anytype, iter: *resp.ValueIterator) !void {
    const subcommand = (try expectArg(iter)).data;

    if (std.ascii.eqlIgnoreCase("get", subcommand)) {
        const key = (try expectArg(iter)).data;

        if (std.ascii.eqlIgnoreCase("dir", key)) {
            if (ctx.cmdline.dir) |dir| {
                const formatted = try std.fmt.allocPrint(ctx.allocator, "*2\r\n$3\r\ndir\r\n${d}\r\n{s}\r\n", .{ dir.len, dir });
                defer ctx.allocator.free(formatted);

                _ = try ctx.connection.stream.write(formatted);
            } else {
                _ = try ctx.connection.stream.write("$-1\r\n");
            }
        } else if (std.ascii.eqlIgnoreCase("dbfilename", key)) {
            if (ctx.cmdline.dbfilename) |dbfilename| {
                const formatted = try std.fmt.allocPrint(ctx.allocator, "*2\r\n$10\r\ndbfilename\r\n${d}\r\n{s}\r\n", .{ dbfilename.len, dbfilename });
                defer ctx.allocator.free(formatted);

                _ = try ctx.connection.stream.write(formatted);
            } else {
                _ = try ctx.connection.stream.write("$-1\r\n");
            }
        }
    } else {
        return error.UnknownSubcommand;
    }
}

fn handleInfo(ctx: anytype, iter: *resp.ValueIterator) !void {
    const section = (try expectArg(iter)).data;

    if (std.ascii.eqlIgnoreCase("replication", section)) {
        if (ctx.cmdline.replicaof) |_| {
            _ = try ctx.connection.stream.write("$10\r\nrole:slave\r\n");
        } else {
            _ = try ctx.connection.stream.write("$11\r\nrole:master\r\n");
        }
    } else {
        return error.UnknownSection;
    }
}
