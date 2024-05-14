const std = @import("std");
const testing = std.testing;

pub const Error = error{ MutateOnEmptyEntry, SlotAlreadyReplaced };

/// The `Arena` allows appending and removing elements that are referred to by
/// `Arena(T, u32, u32).Index`.
pub fn Arena(comptime T: type, comptime IndexType: type, comptime GenerationType: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        unmanaged: Unmanaged,

        const Unmanaged = ArenaUnmanaged(T, IndexType, GenerationType);

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
            self.unmanaged.clear(self.allocator);
        }

        pub fn remove(self: *Self, i: Index) ?Entry {
            return self.unmanaged.remove(i);
        }

        /// Check if an index exists in the arena
        pub fn contains(self: *Self, i: Index) bool {
            return self.unmanaged.contains(i);
        }

        pub fn get(self: *Self, i: Index) ?Entry {
            return self.unmanaged.get(i);
        }

        /// Obtain the data for one field in the arena. Useful if you only to split hot or cold data.
        pub fn getField(self: *Self, i: Index, comptime field: Unmanaged.EntryList.Field) ?std.meta.fieldInfo(T, field).type {
            return self.unmanaged.getField(i, field);
        }

        /// Get a pointer to the data for one field in the arena. Never save this anywhere!
        pub fn getFieldPtr(self: *Self, i: Index, comptime field: Unmanaged.EntryList.Field) ?*std.meta.fieldInfo(T, field).type {
            return self.unmanaged.getFieldPtr(i, field);
        }

        pub fn getUnchecked(self: *Self, i: Index) Entry {
            return self.unmanaged.getUnchecked(i);
        }

        pub fn getFieldPtrUnchecked(self: *Self, i: Index, comptime field: Unmanaged.EntryList.Field) *std.meta.fieldInfo(T, field).type {
            return self.unmanaged.getFieldPtrUnchecked(i, field);
        }

        /// Set the data for one field in the arena, this won't bump the generation. See .mutate(..) for that.
        pub fn setField(self: *Self, i: Index, comptime field: Unmanaged.EntryList.Field, value: std.meta.fieldInfo(T, field).type) !void {
            return self.unmanaged.setField(i, field, value);
        }

        /// Overwrite one arena element with new data.
        pub fn mutate(self: *Self, i: Index, entry: Entry) !void {
            self.unmanaged.mutate(i, entry) catch |err| return err;
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

pub fn ArenaUnmanaged(comptime T: type, comptime IndexType: type, comptime GenerationType: type) type {
    return struct {
        const Self = @This();

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
        const EntryList = std.MultiArrayList(Entry);
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
            self.entries.set(index.index, item);
            self.statuses.items[index.index] = EntryStatus.Occupied(index);
            return index;
        }

        /// Mark all entries as empty and invalidate their data
        pub fn clear(self: *Self, allocator: std.mem.Allocator) void {
            self.entries.shrinkRetainingCapacity(0);
            self.statuses.clearRetainingCapacity();
            self.statuses.expandToCapacity();
            self.entries.setCapacity(allocator, self.statuses.capacity) catch unreachable;

            const end = self.statuses.capacity;
            for (self.statuses.items, 0..) |*status, i| {
                const generation = switch (status.*) {
                    .occupied => |value| value.generation + 1,
                    .empty => |value| value.generation,
                };
                if (i == end - 1) {
                    status.* = EntryStatus.Empty(null, generation);
                } else {
                    status.* = EntryStatus.Empty(i + 1, generation);
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
                    const new_generation = occupant.generation + 1;
                    self.statuses.items[i.index] = EntryStatus.Empty(self.free_list_head, new_generation);
                    self.free_list_head = i.index;
                    self.len -= 1;
                    return self.entries.get(i.index);
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
            return if (self.contains(i)) self.entries.get(i.index) else null;
        }

        /// Obtain the data for one field in the arena. Useful if you only to split hot or cold data.
        pub fn getField(self: *Self, i: Index, comptime field: EntryList.Field) ?std.meta.fieldInfo(T, field).type {
            return if (self.contains(i)) self.entries.items(field)[i.index] else null;
        }

        /// Get a pointer to the data for one field in the arena. Never save this anywhere!
        pub fn getFieldPtr(self: *Self, i: Index, comptime field: EntryList.Field) ?*std.meta.fieldInfo(T, field).type {
            return if (self.contains(i)) &self.entries.items(field)[i.index] else null;
        }

        pub fn getUnchecked(self: *Self, i: Index) Entry {
            std.debug.assert(self.contains(i));
            return self.entries.get(i.index);
        }

        pub fn getFieldPtrUnchecked(self: *Self, i: Index, comptime field: EntryList.Field) *std.meta.fieldInfo(T, field).type {
            std.debug.assert(self.contains(i));
            return &self.entries.items(field)[i.index];
        }

        /// Set the data for one field in the arena, this won't bump the generation. See .mutate(..) for that.
        pub fn setField(self: *Self, i: Index, comptime field: EntryList.Field, value: std.meta.fieldInfo(T, field).type) !void {
            return if (self.contains(i)) {
                self.entries.items(field)[i.index] = value;
            } else error.SlotAlreadyReplaced;
        }

        /// Overwrite one arena element with new data.
        pub fn mutate(self: *Self, i: Index, entry: Entry) !void {
            if (self.contains(i)) {
                self.entries.set(i.index, entry);
            } else return Error.MutateOnEmptyEntry;
        }

        /// Check if the arena is empty
        pub inline fn isEmpty(self: *Self) bool {
            return self.len == 0;
        }

        pub const Iterator = struct {
            ctx: *Self,
            pos: IndexType = 0,

            pub fn next(self: *Iterator) ?Index {
                if ((self.pos) >= self.ctx.len) return null;
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

    // lets mutate by setting field
    try arena.setField(index, .a, 48);
    const entry4 = arena.get(index).?;
    try testing.expect(entry4.a == 48);
    try testing.expect(entry4.b == 47);

    const ptr = arena.getFieldPtr(index, .a).?;
    ptr.* = 49;

    const entry5 = arena.get(index).?;
    try testing.expect(entry5.a == 49);
    try testing.expect(entry5.b == 47);

    // test unchecked methods
    const entry6 = arena.getUnchecked(index);
    try testing.expect(entry6.a == 49);
    try testing.expect(entry6.b == 47);

    const ptr2 = arena.getFieldPtrUnchecked(index, .a);
    ptr2.* = 50;

    const entry7 = arena.getUnchecked(index);
    try testing.expect(entry7.a == 50);
    try testing.expect(entry7.b == 47);
}
