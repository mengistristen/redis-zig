const std = @import("std");
const net = std.net;

const resp = @import("resp.zig");

const Allocator = std.mem.Allocator;

const buff_size: usize = 1024;

fn handleCommand(conn: net.Server.Connection, value: resp.Value) !void {
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
    }
}

fn handleConnection(conn: net.Server.Connection, allocator: Allocator) !void {
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

        var parser = resp.Parser.init(&buffer, allocator);

        if (try parser.next()) |data| {
            defer data.deinit(allocator);

            handleCommand(conn, data) catch {
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

    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection\n", .{});

        _ = try std.Thread.spawn(.{}, handleConnection, .{ connection, allocator });
    }
}
