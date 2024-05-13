const std = @import("std");
const testing = std.testing;

pub const Error = error{MutateOnEmptyEntry};

/// Generally you can set IndexType and GenerationType to be usize or u32
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
        pub fn getByField(self: *Self, i: Index, comptime field: Unmanaged.EntryList.Field) ?std.meta.fieldInfo(T, field).type {
            return switch (self.unmanaged.statuses.items[i.index]) {
                .occupied => if (self.contains(i)) self.unmanaged.entries.items(field)[i.index] else null,
                else => null,
            };
        }

        pub fn setFieldValue(self: *Self, i: Index, comptime field: Unmanaged.EntryList.Field, value: std.meta.fieldInfo(T, field).type) !void {
            return switch (self.unmanaged.statuses.items[i.index]) {
                .occupied => if (self.contains(i)) {
                    self.unmanaged.entries.items(field)[i.index] = value;
                } else error.SlotAlreadyReplaced,
                else => error.MutateOnEmptyEntry,
            };
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

/// Generally you can set IndexType and GenerationType to be usize or u32
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
            return switch (self.statuses.items[i.index]) {
                .occupied => if (self.contains(i)) self.entries.get(i.index) else null,
                else => null,
            };
        }

        /// Obtain the data for one field in the arena. Useful if you only to split hot or cold data.
        pub fn get_by_field(self: *Self, i: Index, comptime field: EntryList.Field) ?std.meta.fieldInfo(T, field).type {
            return switch (self.statuses.items[i.index]) {
                .occupied => if (self.contains(i)) self.entries.items(field)[i.index] else null,
                else => null,
            };
        }

        /// Overwrite one arena element with new data.
        pub fn mutate(self: *Self, i: Index, entry: Entry) !void {
            return switch (self.statuses.items[i.index]) {
                .occupied => self.entries.set(i.index, entry),
                else => return Error.MutateOnEmptyEntry,
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
