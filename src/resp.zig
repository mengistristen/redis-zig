const std = @import("std");
const Allocator = std.mem.Allocator;

const Error = error{
    Malformed,
    InvalidVariant,
    InvalidLength,
    OutOfData,
    Unspecified,
};

const Tag = enum {
    simple_string,
    bulk_string,
    array,
};

pub const SimpleString = struct {
    data: []const u8,
    raw: []const u8,
};

pub const BulkString = struct {
    data: []const u8,
    raw: []const u8,
};

pub const Array = struct {
    data: []Value,
    raw: []const u8,
};

pub const Value = union(Tag) {
    const Self = @This();

    simple_string: SimpleString,
    bulk_string: BulkString,
    array: Array,

    pub fn unwrapBulkString(self: Self) !BulkString {
        switch (self) {
            .bulk_string => |value| {
                return value;
            },
            else => return Error.InvalidVariant,
        }
    }
    pub fn unwrapArray(self: Self) !Array {
        switch (self) {
            .array => |value| {
                return value;
            },
            else => return Error.InvalidVariant,
        }
    }
    pub fn deinit(self: Self, allocator: Allocator) void {
        switch (self) {
            .array => |value| {
                for (value.data) |item| {
                    item.deinit(allocator);
                }

                allocator.free(value.data);
            },
            else => {},
        }
    }
};

pub const ValueIterator = struct {
    values: []Value,
    index: usize = 0,

    pub fn next(self: *ValueIterator) ?Value {
        if (self.index >= self.values.len) {
            return null;
        }

        const result = self.values[self.index];

        self.index += 1;

        return result;
    }
};

pub const Parser = struct {
    const Self = @This();

    data: []const u8,
    index: usize = 0,
    allocator: Allocator,

    pub fn init(data: []const u8, allocator: Allocator) Self {
        return Self{ .data = data, .allocator = allocator };
    }
    pub fn next(self: *Self) Error!?Value {
        return self.parseValue();
    }
    fn advance(self: *Self) ?u8 {
        if (self.index >= self.data.len) {
            return null;
        }

        const result = self.data[self.index];

        self.index += 1;

        return result;
    }
    fn advanceSection(self: *Self) ![]const u8 {
        const start = self.index;

        while (self.peek() != '\r') {
            _ = self.advance();
        }

        if (self.peek() == '\r') {
            _ = self.advance();
        } else {
            return Error.Malformed;
        }

        if (self.peek() == '\n') {
            _ = self.advance();
        } else {
            return Error.Malformed;
        }

        return self.data[start .. self.index - 2];
    }
    fn peek(self: Self) ?u8 {
        if (self.index >= self.data.len) {
            return null;
        }

        return self.data[self.index];
    }
    fn parseValue(self: *Self) Error!?Value {
        if (self.advance()) |char| {
            switch (char) {
                '+' => return self.parseSimpleString(),
                '$' => return self.parseBulkString(),
                '*' => return self.parseArray(),
                else => return Error.Malformed,
            }
        }

        return null;
    }
    fn parseSimpleString(self: *Self) Error!?Value {
        const start = self.index;

        return Value{ .simple_string = SimpleString{
            .data = try self.advanceSection(),
            .raw = self.data[start - 1 .. self.index],
        } };
    }
    fn parseBulkString(self: *Self) Error!?Value {
        const start = self.index;
        const len = std.fmt.parseInt(usize, try self.advanceSection(), 10) catch {
            return Error.InvalidLength;
        };

        if (self.index + len + 2 > self.data.len or self.data[self.index + len] != '\r' or self.data[self.index + len + 1] != '\n') {
            return Error.Malformed;
        }

        const data = self.data[self.index .. self.index + len];

        self.index += len + 2;

        return Value{ .bulk_string = BulkString{
            .data = data,
            .raw = self.data[start - 1 .. self.index],
        } };
    }
    fn parseArray(self: *Self) Error!?Value {
        const start = self.index;

        var list = std.ArrayList(Value).init(self.allocator);
        defer list.deinit();

        const len = std.fmt.parseInt(usize, try self.advanceSection(), 10) catch {
            return Error.InvalidLength;
        };

        for (0..len) |_| {
            if (try self.parseValue()) |resp| {
                list.append(resp) catch {
                    return Error.Unspecified;
                };
            } else {
                return Error.OutOfData;
            }
        }

        const data = list.toOwnedSlice() catch {
            return Error.Unspecified;
        };

        return Value{ .array = Array{
            .data = data,
            .raw = self.data[start - 1 .. self.index],
        } };
    }
};

test "parser parses simple strings" {
    var parser = Parser.init("+data\r\n", std.testing.allocator);

    if (try parser.next()) |result| {
        defer result.deinit(std.testing.allocator);

        switch (result) {
            .simple_string => |value| {
                try std.testing.expectEqualStrings("data", value.data);
            },
            else => unreachable,
        }
    } else {
        try std.testing.expect(false);
    }
}

test "parser parses bulk strings" {
    var parser = Parser.init("$4\r\ndata\r\n", std.testing.allocator);

    if (try parser.next()) |result| {
        defer result.deinit(std.testing.allocator);

        switch (result) {
            .bulk_string => |value| {
                try std.testing.expectEqualStrings("data", value.data);
            },
            else => unreachable,
        }
    } else {
        try std.testing.expect(false);
    }
}

test "parser parses arrays" {
    var parser = Parser.init("*2\r\n+data\r\n$4\r\ndata\r\n", std.testing.allocator);

    if (try parser.next()) |result| {
        defer result.deinit(std.testing.allocator);

        switch (result) {
            .array => |value| {
                switch (value.data[0]) {
                    .simple_string => |value_inner| {
                        try std.testing.expectEqualStrings("data", value_inner.data);
                    },
                    else => unreachable,
                }

                switch (value.data[1]) {
                    .bulk_string => |value_inner| {
                        try std.testing.expectEqualStrings("data", value_inner.data);
                    },
                    else => unreachable,
                }
            },
            else => unreachable,
        }
    } else {
        try std.testing.expect(false);
    }
}

test "parser parses nested arrays" {
    var parser = Parser.init("*2\r\n*1\r\n+data\r\n+atad\r\n", std.testing.allocator);

    if (try parser.next()) |result| {
        defer result.deinit(std.testing.allocator);

        switch (result) {
            .array => |value| {
                switch (value.data[0]) {
                    .array => |value_inner| {
                        switch (value_inner.data[0]) {
                            .simple_string => |str| {
                                try std.testing.expectEqualStrings("data", str.data);
                            },
                            else => unreachable,
                        }
                    },
                    else => unreachable,
                }

                switch (value.data[1]) {
                    .simple_string => |str| {
                        try std.testing.expectEqualStrings("atad", str.data);
                    },
                    else => unreachable,
                }
            },
            else => unreachable,
        }
    }
}

test "parser parses multiple values" {
    var parser = Parser.init("$4\r\ndata\r\n+data\r\n", std.testing.allocator);

    if (try parser.next()) |result| {
        defer result.deinit(std.testing.allocator);

        switch (result) {
            .bulk_string => |value| {
                try std.testing.expectEqualStrings("data", value.data);
            },
            else => unreachable,
        }
    } else {
        try std.testing.expect(false);
    }

    if (try parser.next()) |result| {
        defer result.deinit(std.testing.allocator);

        switch (result) {
            .simple_string => |value| {
                try std.testing.expectEqualStrings("data", value.data);
            },
            else => unreachable,
        }
    } else {
        try std.testing.expect(false);
    }
}
