const std = @import("std");
const testing = std.testing;

pub const Error = error{ MutateOnEmptyEntry, SlotAlreadyReplaced };

/// The `Arena` allows appending and removing elements that are referred to by
/// `Arena(T, u32, u32).Index`.
pub fn Arena(comptime T: type, comptime InputIndexType: type, comptime InputGenerationType: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        unmanaged: Unmanaged,

        const Unmanaged = ArenaUnmanaged(T, InputIndexType, InputGenerationType);

        pub const Index = Unmanaged.Index;
        pub const Entry = Unmanaged.Entry;

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .unmanaged = Unmanaged{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.unmanaged.deinit(self.allocator);
        }

        pub fn capacity(self: *const Self) Unmanaged.IndexType {
            return self.unmanaged.capacity();
        }

        pub fn append(self: *Self, item: T) !Index {
            return self.unmanaged.append(self.allocator, item);
        }

        pub fn clear(self: *Self) void {
            self.unmanaged.clear();
        }

        pub fn remove(self: *Self, i: Index) ?Entry {
            return self.unmanaged.remove(i);
        }

        /// Check if an index exists in the arena
        pub fn contains(self: *Self, i: Index) bool {
            return self.unmanaged.contains(i);
        }

        /// Obtain all the data for one entry in the arena.
        pub fn get(self: *Self, i: Index) ?Entry {
            return self.unmanaged.get(i);
        }

        /// Obtain a pointer to the data for one entry in the arena. Never save this anywhere!
        pub fn getPtr(self: *Self, i: Index) ?*Entry {
            return self.unmanaged.getPtr(i);
        }

        pub fn getUnchecked(self: *Self, i: Index) Entry {
            return self.unmanaged.getUnchecked(i);
        }

        pub fn getPtrUnchecked(self: *Self, i: Index) *Entry {
            return self.unmanaged.getPtrUnchecked(i);
        }

        /// Overwrite one arena element with new data.
        pub fn mutate(self: *Self, i: Index, entry: Entry) !void {
            self.unmanaged.mutate(i, entry) catch |err| return err;
        }

        pub fn getHandleByIndex(self: *Self, index: Unmanaged.IndexType) ?Index {
            return self.unmanaged.getHandleByIndex(index);
        }

        /// Check if the arena is empty
        pub fn isEmpty(self: *Self) bool {
            return self.unmanaged.isEmpty();
        }

        pub const Iterator = Unmanaged.Iterator;

        pub fn iterator(self: *Self) Iterator {
            return self.unmanaged.iterator();
        }
    };
}

pub fn ArenaUnmanaged(comptime T: type, comptime InputIndexType: type, comptime InputGenerationType: type) type {
    return struct {
        const Self = @This();

        pub const IndexType = InputIndexType;
        pub const GenerationType = InputGenerationType;

        pub const Index = struct {
            index: IndexType,
            generation: GenerationType,

            pub fn fromParts(index: IndexType, generation: GenerationType) Index {
                return .{ .index = index, .generation = generation };
            }

            pub fn equals(a: Index, b: Index) bool {
                return a.index == b.index and a.generation == b.generation;
            }
        };

        const EmptyEntry = struct {
            next_free: ?IndexType,
            generation: GenerationType,
        };

        const EntryStatus = union(enum) {
            occupied: Index,
            empty: EmptyEntry,

            fn Occupied(i: Index) EntryStatus {
                return .{ .occupied = i };
            }

            fn Empty(next_free: ?IndexType, generation: GenerationType) EntryStatus {
                return .{ .empty = EmptyEntry{ .next_free = next_free, .generation = generation } };
            }
        };

        pub const Entry = T;
        const EntryList = std.ArrayListUnmanaged(Entry);
        const StatusList = std.ArrayListUnmanaged(EntryStatus);

        entries: EntryList = .{},
        statuses: StatusList = .{},
        free_list_head: ?IndexType = null,
        len: IndexType = 0,

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.entries.deinit(allocator);
            self.statuses.deinit(allocator);
        }

        /// Get the current capacity of the arena
        pub fn capacity(self: *const Self) IndexType {
            return @intCast(self.statuses.items.len);
        }

        /// Allocate space for a new entry
        fn alloc(self: *Self, allocator: std.mem.Allocator) !Index {
            if (self.free_list_head) |i| {
                switch (self.statuses.items[i]) {
                    .occupied => {
                        std.debug.panic("Corrupted free list tried using index {d}", .{i});
                    },
                    .empty => |value| {
                        self.free_list_head = value.next_free;
                        self.len += 1;
                        return Index.fromParts(i, value.generation);
                    },
                }
            } else {
                const i = self.statuses.items.len;
                try self.statuses.append(allocator, EntryStatus.Empty(@intCast(i), 0));
                try self.entries.resize(allocator, self.statuses.capacity);
                self.len += 1;
                return Index.fromParts(@intCast(i), 0);
            }
        }

        /// Extend the list by 1 element. Allocates more memory as necessary.
        pub fn append(self: *Self, allocator: std.mem.Allocator, item: T) !Index {
            const index = try self.alloc(allocator);
            self.entries.items[index.index] = item;
            self.statuses.items[index.index] = EntryStatus.Occupied(index);
            return index;
        }

        /// Mark all entries as empty and invalidate their data
        pub fn clear(self: *Self) void {
            self.entries.clearRetainingCapacity();
            self.statuses.clearRetainingCapacity();
            self.statuses.expandToCapacity();
            self.entries.expandToCapacity();

            const end = self.statuses.capacity;
            for (self.statuses.items, 0..) |*status, i| {
                const generation = switch (status.*) {
                    .occupied => |value| if (value.generation == std.math.maxInt(GenerationType)) 0 else value.generation + 1,
                    .empty => |value| value.generation,
                };
                if (i == end - 1) {
                    status.* = EntryStatus.Empty(null, generation);
                } else {
                    status.* = EntryStatus.Empty(@as(IndexType, @intCast(i)) + 1, generation);
                }
            }

            self.free_list_head = 0;
            self.len = 0;
        }

        /// Removes the element at the specified index and returns it
        /// if it exists. Otherwise returns null.
        /// This operation is O(1).
        pub fn remove(self: *Self, i: Index) ?Entry {
            // if (i.index >= self.len) {
            //     return null;
            // }
            const entry_to_delete = self.statuses.items[i.index];
            return switch (entry_to_delete) {
                .occupied => |occupant| if (occupant.equals(i)) {
                    const new_generation = if (occupant.generation == std.math.maxInt(GenerationType)) 0 else occupant.generation + 1;
                    self.statuses.items[i.index] = EntryStatus.Empty(self.free_list_head, new_generation);
                    self.free_list_head = i.index;
                    self.len -= 1;
                    return self.entries.items[i.index];
                } else return null,
                else => null,
            };
        }

        /// Check if an index exists in the arena
        pub fn contains(self: *Self, i: Index) bool {
            return switch (self.statuses.items[i.index]) {
                .occupied => |occupant| occupant.equals(i),
                else => false,
            };
        }

        /// Obtain all the data for one entry in the arena.
        pub fn get(self: *Self, i: Index) ?Entry {
            return if (self.contains(i)) self.entries.items[i.index] else null;
        }

        /// Get a pointer to the data for an entry in the arena. Never save this anywhere!
        pub fn getPtr(self: *Self, i: Index) ?*Entry {
            return if (self.contains(i)) &self.entries[i.index] else null;
        }

        pub fn getUnchecked(self: *Self, i: Index) Entry {
            std.debug.assert(self.contains(i));
            return self.entries.items[i.index];
        }

        pub fn getPtrUnchecked(self: *Self, i: Index) *Entry {
            std.debug.assert(self.contains(i));
            return &self.entries.items[i.index];
        }

        /// Overwrite one arena element with new data.
        pub fn mutate(self: *Self, i: Index, entry: Entry) !void {
            if (self.contains(i)) {
                self.entries.items[i.index] = entry;
            } else return Error.MutateOnEmptyEntry;
        }

        pub fn getHandleByIndex(self: *Self, index: IndexType) ?Index {
            if (index >= self.statuses.items.len) {
                return null;
            }
            return switch (self.statuses.items[index]) {
                .occupied => |occupant| {
                    return occupant;
                },
                else => return null,
            };
        }

        /// Check if the arena is empty
        pub inline fn isEmpty(self: *Self) bool {
            return self.len == 0;
        }

        pub const Iterator = struct {
            ctx: *Self,
            pos: IndexType = 0,

            pub fn next(self: *Iterator) ?Index {
                if (self.pos >= self.ctx.len) return null;
                return switch (self.ctx.statuses.items[self.pos]) {
                    .empty => {
                        self.pos += 1;
                        return self.next();
                    },
                    .occupied => |occupant| {
                        self.pos += 1;
                        return occupant;
                    },
                };
            }
        };

        pub fn iterator(self: *Self) Iterator {
            return Self.Iterator{ .ctx = self };
        }
    };
}

test {
    const TestStruct = struct {
        a: u32,
        b: u32,
    };
    var arena = Arena(TestStruct, u32, u32).init(std.heap.page_allocator);
    defer arena.deinit();
    try testing.expect(arena.isEmpty());
    try testing.expect(arena.capacity() == 0);

    const index = try arena.append(TestStruct{
        .a = 42,
        .b = 43,
    });
    try testing.expect(index.index == 0);
    try testing.expect(arena.get(index) != null);
    const entry = arena.get(index).?;
    try testing.expect(entry.a == 42);
    try testing.expect(entry.b == 43);

    // lets try another item
    const index2 = try arena.append(TestStruct{
        .a = 44,
        .b = 45,
    });

    try testing.expect(index2.index == 1);
    try testing.expect(arena.get(index2) != null);
    const entry2 = arena.get(index2).?;
    try testing.expect(entry2.a == 44);
    try testing.expect(entry2.b == 45);

    // lets try and mutate and check the generation
    try arena.mutate(index, TestStruct{
        .a = 46,
        .b = 47,
    });

    const entry3 = arena.get(index).?;
    try testing.expect(entry3.a == 46);
    try testing.expect(entry3.b == 47);

    // capacity check
    try testing.expect(arena.capacity() == 2);
    // is empty check
    try testing.expect(!arena.isEmpty());

    // test unchecked methods
    const entry6 = arena.getUnchecked(index);
    try testing.expect(entry6.a == 46);
    try testing.expect(entry6.b == 47);

    // try mutate
    try arena.mutate(index, TestStruct{
        .a = 51,
        .b = 52,
    });
    try testing.expect(arena.get(index).?.a == 51);
    try testing.expect(arena.get(index).?.b == 52);

    // check if remove
    try testing.expect(arena.remove(index) != null);
    try testing.expect(arena.get(index) == null);

    // check generation when adding new
    const index3 = try arena.append(TestStruct{
        .a = 53,
        .b = 54,
    });
    try testing.expect(index3.index == 0);
    try testing.expect(index3.generation == 1);

    //  test get handle by index
    try testing.expect(arena.getHandleByIndex(0) != null);
    _ = arena.remove(arena.getHandleByIndex(1).?);
    try testing.expect(arena.getHandleByIndex(1) == null);

    // check clear
    arena.clear();
    try testing.expect(arena.isEmpty());
}
