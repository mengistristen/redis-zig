const std = @import("std");
const net = std.net;

const command = @import("command.zig");
const config = @import("config.zig");
const datastore = @import("datastore.zig");
const resp = @import("resp.zig");

const Allocator = std.mem.Allocator;

const buff_size: usize = 1024;

fn Context(comptime T: type) type {
    return struct {
        connection: net.Server.Connection,
        allocator: Allocator,
        configuration: config.Config,
        datastore: *T,
    };
}

fn handleConnection(
    comptime T: type,
    ctx: Context(T),
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

            command.handle(@TypeOf(ctx), ctx, data) catch {
                _ = try ctx.connection.stream.write("-failed to process command\r\n");
            };
        }
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var store = datastore.InMemoryDataStore.init(allocator);
    defer store.deinit();

    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    const configuration = try config.process(allocator);
    defer configuration.deinit(allocator);

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection\n", .{});

        const thread = try std.Thread.spawn(.{}, handleConnection, .{ @TypeOf(store), .{
            .allocator = allocator,
            .configuration = configuration,
            .connection = connection,
            .datastore = &store,
        } });

        thread.detach();
    }
}
