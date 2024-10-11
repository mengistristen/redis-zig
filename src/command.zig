const std = @import("std");
const net = std.net;

const Allocator = std.mem.Allocator;

const resp = @import("resp.zig");
const datastore = @import("datastore.zig");

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

pub fn handle(comptime T: type, conn: net.Server.Connection, allocator: Allocator, value: resp.Value, store: *T) !void {
    const args = (try value.unwrapArray()).data;
    var iter = resp.ValueIterator{
        .values = args,
    };

    const command = (try expectArg(&iter)).data;

    if (std.ascii.eqlIgnoreCase("ping", command)) {
        _ = try conn.stream.write("+PONG\r\n");
    } else if (std.ascii.eqlIgnoreCase("echo", command)) {
        const paramRaw = (try expectArg(&iter)).raw;

        _ = try conn.stream.write(paramRaw);
    } else if (std.ascii.eqlIgnoreCase("get", command)) {
        const key = (try expectArg(&iter)).data;

        if (datastore.get(T, store, key)) |data| {
            const formatted = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ data.len, data });
            defer allocator.free(formatted);

            _ = try conn.stream.write(formatted);
        } else {
            _ = try conn.stream.write("$-1\r\n");
        }
    } else if (std.ascii.eqlIgnoreCase("set", command)) {
        const k = (try expectArg(&iter)).data;
        const v = (try expectArg(&iter)).data;

        var expiry: ?i64 = null;

        if (try acceptArg(&iter)) |arg| {
            if (std.ascii.eqlIgnoreCase("px", arg.data)) {
                const amount_str = (try expectArg(&iter)).data;

                const amount = try std.fmt.parseInt(i64, amount_str, 10);

                expiry = amount;
            }
        }

        if (datastore.set(T, store, k, v, expiry)) |_| {
            _ = try conn.stream.write("+OK\r\n");
        } else |_| {
            _ = try conn.stream.write("-error setting value\r\n");
        }
    }
}
