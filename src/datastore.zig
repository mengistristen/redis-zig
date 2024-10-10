const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ThreadSafeHashMap = struct {
    const Self = @This();

    allocator: Allocator,
    mutex: std.Thread.Mutex,
    map: std.StringHashMap([]u8),

    pub fn init(allocator: Allocator) Self {
        return Self{
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .map = std.StringHashMap([]u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var iterator = self.map.iterator();

        while (iterator.next()) |entry| {
            self.allocator.destroy(entry.key_ptr);
            self.allocator.destroy(entry.value_ptr);
        }

        self.map.deinit();
    }

    pub fn get(self: *Self, key: []const u8) ?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.map.get(key);
    }

    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const owned_key = try self.allocator.dupe(u8, key);
        const owned_value = try self.allocator.dupe(u8, value);

        if (try self.map.fetchPut(owned_key, owned_value)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value);
        }
    }
};
