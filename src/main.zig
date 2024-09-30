const std = @import("std");
const net = std.net;

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

        try stdout.print("accepted new connection", .{});

        const reader = connection.stream.reader();
        var buffer: [256]u8 = undefined;

        while (try reader.readUntilDelimiterOrEof(&buffer, '\n')) |_| {
            _ = try connection.stream.write("+PONG\r\n");
        }

        connection.stream.close();
    }
}
