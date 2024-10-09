const std = @import("std");
const net = std.net;

const resp = @import("resp.zig");

const Allocator = std.mem.Allocator;

const buff_size: usize = 1024;

fn handleConnection(conn: net.Server.Connection, allocator: Allocator) !void {
    const reader = conn.stream.reader();
    var buffer: [buff_size]u8 = undefined;

    while (try reader.read(&buffer) > 0) {
        var parser = resp.Parser.init(&buffer, allocator);

        if (try parser.next()) |data| {
            defer data.deinit(allocator);

            switch (data) {
                .array => |input| {
                    if (input.data.len < 1) {
                        return error.Todo;
                    }

                    const commandBulk = try input.data[0].unwrapBulkString();
                    const commandLower = try std.ascii.allocLowerString(allocator, commandBulk.data);
                    defer allocator.free(commandLower);

                    std.debug.print("command: {s}\n", .{commandLower});

                    if (std.mem.eql(u8, "ping", commandLower)) {
                        _ = try conn.stream.write("+PONG\r\n");
                    } else if (std.mem.eql(u8, "echo", commandLower)) {
                        if (input.data.len < 2) {
                            return error.Todo;
                        }

                        const paramBulk = try input.data[1].unwrapBulkString();

                        _ = try conn.stream.write(paramBulk.raw);
                    }
                },
                else => {
                    return error.Todo;
                },
            }
        }
        _ = try conn.stream.write("+PONG\r\n");
    }

    conn.stream.close();
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
