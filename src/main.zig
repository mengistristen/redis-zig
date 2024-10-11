const std = @import("std");
const net = std.net;

const resp = @import("resp.zig");
const datastore = @import("datastore.zig");

const Allocator = std.mem.Allocator;

const buff_size: usize = 1024;

fn get(comptime T: type, store: *T, key: []const u8) ?[]u8 {
    return store.get(key);
}

fn set(comptime T: type, store: *T, key: []const u8, value: []const u8, expiry: ?i64) !void {
    try store.set(key, value, expiry);
}

fn handleCommand(comptime T: type, conn: net.Server.Connection, allocator: Allocator, value: resp.Value, store: *T) !void {
    const args = (try value.unwrapArray()).data;

    if (args.len < 1) {
        return error.MissingCommand;
    }

    const command = (try args[0].unwrapBulkString()).data;

    if (std.ascii.eqlIgnoreCase("ping", command)) {
        _ = try conn.stream.write("+PONG\r\n");
    } else if (std.ascii.eqlIgnoreCase("echo", command)) {
        if (args.len < 2) {
            return error.MissingArgument;
        }

        const paramRaw = (try args[1].unwrapBulkString()).raw;

        _ = try conn.stream.write(paramRaw);
    } else if (std.ascii.eqlIgnoreCase("get", command)) {
        if (args.len < 2) {
            return error.MissingArgument;
        }

        const key = (try args[1].unwrapBulkString()).data;

        if (get(T, store, key)) |data| {
            const formatted = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ data.len, data });
            defer allocator.free(formatted);

            _ = try conn.stream.write(formatted);
        } else {
            _ = try conn.stream.write("$-1\r\n");
        }
    } else if (std.ascii.eqlIgnoreCase("set", command)) {
        if (args.len < 3) {
            return error.MissingArgument;
        }

        const k = (try args[1].unwrapBulkString()).data;
        const v = (try args[2].unwrapBulkString()).data;

        if (set(T, store, k, v, null)) |_| {
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
