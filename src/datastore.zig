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
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
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

        if (self.map.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value);
        }

        const owned_key = try self.allocator.dupe(u8, key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_key);
        errdefer self.allocator.free(owned_value);

        try self.map.put(owned_key, owned_value);
    }
};

test "hash map can store multiple values" {
    var map = ThreadSafeHashMap.init(std.testing.allocator);
    defer map.deinit();

    try map.set("key1", "value1");
    try map.set("key2", "value2");

    if (map.get("key1")) |value| {
        try std.testing.expectEqualStrings("value1", value);
    } else unreachable;

    if (map.get("key2")) |value| {
        try std.testing.expectEqualStrings("value2", value);
    } else unreachable;
}

test "hash map can override keys" {
    var map = ThreadSafeHashMap.init(std.testing.allocator);
    defer map.deinit();

    try map.set("key", "value");
    try map.set("key", "value2");
}
