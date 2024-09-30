const std = @import("std");
const net = std.net;

fn handleConnection(conn: net.Server.Connection) !void {
    const reader = conn.stream.reader();
    var buffer: [256]u8 = undefined;

    while (try reader.read(&buffer) > 0) {
        _ = try conn.stream.write("+PONG\r\n");
    }

    conn.stream.close();
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    // Uncomment this block to pass the first stage
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        _ = try std.Thread.spawn(.{}, handleConnection, .{connection});

        try stdout.print("accepted new connection", .{});
    }
}
