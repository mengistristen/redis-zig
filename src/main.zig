const std = @import("std");
const net = std.net;

const resp = @import("resp.zig");
const datastore = @import("datastore.zig");

const Allocator = std.mem.Allocator;

const buff_size: usize = 1024;

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

fn handleCommand(comptime T: type, conn: net.Server.Connection, allocator: Allocator, value: resp.Value, store: *T) !void {
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

fn handleConnection(conn: net.Server.Connection, allocator: Allocator, store: *datastore.ThreadSafeHashMap) !void {
    defer conn.stream.close();

    const reader = conn.stream.reader();
    var buffer: [buff_size]u8 = undefined;

    while (true) {
        const bytes_read = reader.read(&buffer) catch |err| {
            if (err == error.ConnectionResetByPeer) {
                break;
            } else {
                return err;
            }
        };

        if (bytes_read == 0) {
            break;
        }

        var parser = resp.Parser.init(buffer[0..bytes_read], allocator);

        if (try parser.next()) |data| {
            defer data.deinit(allocator);

            handleCommand(datastore.ThreadSafeHashMap, conn, allocator, data, store) catch {
                _ = try conn.stream.write("-failed to process command\r\n");
            };
        }
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var store = datastore.ThreadSafeHashMap.init(allocator);
    defer store.deinit();

    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection\n", .{});

        const thread = try std.Thread.spawn(.{}, handleConnection, .{ connection, allocator, &store });

        thread.detach();
    }
}
