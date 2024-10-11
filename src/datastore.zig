const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn get(comptime T: type, store: *T, key: []const u8) ?[]u8 {
    return store.get(key);
}

pub fn set(comptime T: type, store: *T, key: []const u8, value: []const u8, expiry: ?i64) !void {
    try store.set(key, value, expiry);
}

const ExpiringValue = struct {
    value: []u8,
    expiration: ?i64,
};

pub const ThreadSafeHashMap = struct {
    const Self = @This();

    allocator: Allocator,
    mutex: std.Thread.Mutex,
    map: std.StringHashMap(*ExpiringValue),

    pub fn init(allocator: Allocator) Self {
        return Self{
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .map = std.StringHashMap(*ExpiringValue).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var iterator = self.map.iterator();

        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*.value);

            self.allocator.destroy(entry.value_ptr.*);
        }

        self.map.deinit();
    }

    pub fn get(self: *Self, key: []const u8) ?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.map.get(key)) |data| {
            if (data.expiration) |timestamp| {
                if (timestamp < std.time.milliTimestamp()) {
                    return null;
                }
            }

            return data.value;
        }

        return null;
    }

    pub fn set(self: *Self, key: []const u8, value: []const u8, expiry: ?i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.map.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value.value);

            self.allocator.destroy(kv.value);
        }

        const owned_key = try self.allocator.dupe(u8, key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_key);
        errdefer self.allocator.free(owned_value);

        const data = try self.allocator.create(ExpiringValue);
        errdefer self.allocator.destroy(data);

        data.value = owned_value;

        if (expiry) |amount| {
            data.expiration = std.time.milliTimestamp() + amount;
        } else {
            data.expiration = null;
        }

        try self.map.put(owned_key, data);
    }
};

test "hash map can store multiple values" {
    var map = ThreadSafeHashMap.init(std.testing.allocator);
    defer map.deinit();

    try map.set("key1", "value1", null);
    try map.set("key2", "value2", null);

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

    try map.set("key", "value", null);
    try map.set("key", "value2", null);
}

test "hash map keys can expire" {
    var map = ThreadSafeHashMap.init(std.testing.allocator);
    defer map.deinit();

    try map.set("key", "value", 1000);

    std.Thread.sleep(2000 * std.time.ns_per_ms);

    try std.testing.expect(map.get("key") == null);
}
