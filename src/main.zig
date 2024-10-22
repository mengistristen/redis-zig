const std = @import("std");
const net = std.net;

const args = @import("args.zig");
const command = @import("command.zig");
const datastore = @import("datastore.zig");
const resp = @import("resp.zig");

const Allocator = std.mem.Allocator;

const buff_size: usize = 1024;

fn handleConnection(
    ctx: anytype,
) !void {
    defer ctx.connection.stream.close();

    const reader = ctx.connection.stream.reader();
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

        var parser = resp.Parser.init(buffer[0..bytes_read], ctx.allocator);

        if (try parser.next()) |data| {
            defer data.deinit(ctx.allocator);

            command.handle(ctx, data) catch {
                _ = try ctx.connection.stream.write("-failed to process command\r\n");
            };
        }
    }
}

pub fn main() !void {
    var args_iter = std.process.args();

    const stdout = std.io.getStdOut().writer();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var store = datastore.InMemoryDataStore.init(allocator);
    defer store.deinit();

    const cmdline = args.process(@TypeOf(args_iter), &args_iter, allocator) catch {
        args.printHelp();
        std.process.exit(1);
    };
    defer cmdline.deinit(allocator);

    var port: u16 = 6379;

    if (cmdline.port) |config_port| {
        port = config_port;
    }

    const address = try net.Address.resolveIp("127.0.0.1", port);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection\n", .{});

        const thread = try std.Thread.spawn(.{}, handleConnection, .{.{
            .allocator = allocator,
            .cmdline = cmdline,
            .connection = connection,
            .datastore = &store,
        }});

        thread.detach();
    }
}
