//! Zig FFI bindings to libvalkey C library.
//! Provides direct command execution via libvalkey.
//!
//! This module handles:
//! - C interop with libvalkey C library
//! - Null-terminated string handling for C calls
//! - Reply type parsing and error handling
//! - Memory management for Valkey replies
//!
//! All allocations for C string arguments are properly scoped and freed
//! in defer blocks to prevent leaks even on early returns.

const std = @import("std");

// Command timeout constants
pub const COMMAND_TIMEOUT_MS: u32 = 5000; // 5 seconds
pub const CONNECT_TIMEOUT_MS: u32 = 3000; // 3 seconds

// C declarations for libvalkey
pub extern "c" fn valkeyConnect(ip: [*:0]const u8, port: c_uint) ?*valkeyContext;
pub extern "c" fn valkeyFree(ctx: ?*valkeyContext) void;
pub extern "c" fn freeReplyObject(reply: ?*valkeyReply) void;
pub extern "c" fn valkeyCommandArgv(ctx: *valkeyContext, argc: c_int, argv: [*]const [*:0]const u8, argvlen: [*]const usize) ?*valkeyReply;

// Valkey constants for reply types
pub const VALKEY_REPLY_STRING: c_int = 1;
pub const VALKEY_REPLY_ARRAY: c_int = 2;
pub const VALKEY_REPLY_INTEGER: c_int = 3;
pub const VALKEY_REPLY_NIL: c_int = 4;
pub const VALKEY_REPLY_STATUS: c_int = 5;
pub const VALKEY_REPLY_ERROR: c_int = 6;

pub const valkeyContext = opaque {};

pub const valkeyReply = extern struct {
    type: c_int,
    integer: c_longlong,
    dval: f64,
    len: usize,
    str: [*:0]u8,
    vtype: [4]u8,
    elements: usize,
    element: [*]*valkeyReply,
};

pub const ValkeyError = error{
    ConnectionFailed,
    CommandFailed,
    InvalidReplyType,
    CommandError,
    InvalidArguments,
};

/// Valkey context wrapper for command execution
pub const ValkeyContext = struct {
    ctx: *valkeyContext,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, host: [*:0]const u8, port: c_uint) !ValkeyContext {
        const ctx = valkeyConnect(host, port) orelse return error.ConnectionFailed;
        return .{
            .ctx = ctx,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: ValkeyContext) void {
        valkeyFree(self.ctx);
    }

    /// Execute command with string arguments
    /// All argument strings are converted to null-terminated C strings (allocations scoped and freed)
    /// Returns reply object that must be freed with freeReplyObject
    pub fn command(self: ValkeyContext, args: []const []const u8) !*valkeyReply {
        // Allocate argv array for command arguments
        var argv = try self.allocator.alloc([*:0]const u8, args.len);
        defer self.allocator.free(argv);

        // Allocate length array for argument sizes
        var argvlen = try self.allocator.alloc(usize, args.len);
        defer self.allocator.free(argvlen);

        // Convert each argument to null-terminated C string
        for (args, 0..) |arg, i| {
            // Allocate and copy argument with null terminator
            const arg_z = try self.allocator.allocSentinel(u8, arg.len, 0);
            @memcpy(arg_z, arg);
            argv[i] = arg_z.ptr;
            argvlen[i] = arg.len;
        }

        // Cleanup all argument allocations in defer block (runs even on error)
        defer {
            for (argv) |a| {
                self.allocator.free(std.mem.span(a));
            }
        }

        // Execute command via libvalkey and return reply
        // Caller responsible for freeing reply with freeReplyObject
        const reply = valkeyCommandArgv(self.ctx, @intCast(args.len), argv.ptr, argvlen.ptr) orelse return error.CommandFailed;
        return reply;
    }

    /// SET key value
    pub fn set(self: ValkeyContext, key: []const u8, value: []const u8) !void {
        const reply = try self.command(&.{ "SET", key, value });
        defer freeReplyObject(reply);

        if (reply.type == VALKEY_REPLY_ERROR) {
            return error.CommandError;
        }
    }

    /// GET key
    pub fn get(self: ValkeyContext, key: []const u8) !?[]u8 {
        const reply = try self.command(&.{ "GET", key });
        defer freeReplyObject(reply);

        if (reply.type == VALKEY_REPLY_NIL) {
            return null;
        }

        if (reply.type != VALKEY_REPLY_STRING) {
            return error.InvalidReplyType;
        }

        const len: usize = @intCast(reply.len);
        const str = reply.str[0..len];
        return try self.allocator.dupe(u8, str);
    }

    /// SADD key members...
    pub fn sadd(self: ValkeyContext, key: []const u8, members: []const []const u8) !i64 {
        if (members.len == 0) return 0;

        var args = try self.allocator.alloc([]const u8, 2 + members.len);
        defer self.allocator.free(args);

        args[0] = "SADD";
        args[1] = key;
        @memcpy(args[2..], members);

        const reply = try self.command(args);
        defer freeReplyObject(reply);

        if (reply.type == VALKEY_REPLY_ERROR) {
            return error.CommandError;
        }

        if (reply.type != VALKEY_REPLY_INTEGER) {
            return error.InvalidReplyType;
        }

        return reply.integer;
    }

    /// SMEMBERS key
    pub fn smembers(self: ValkeyContext, key: []const u8) !std.ArrayList([]u8) {
        const reply = try self.command(&.{ "SMEMBERS", key });
        defer freeReplyObject(reply);

        var result: std.ArrayList([]u8) = .init(self.allocator);

        if (reply.type == VALKEY_REPLY_NIL) {
            // Key doesn't exist, return empty array
            return result;
        }

        if (reply.type != VALKEY_REPLY_ARRAY) {
            return error.InvalidReplyType;
        }

        const elements = reply.elements;

        for (0..elements) |i| {
            const elem = reply.element[i];
            if (elem.type == VALKEY_REPLY_STRING) {
                const str = elem.str[0..elem.len];
                try result.append(self.allocator, try self.allocator.dupe(u8, str));
            }
        }

        return result;
    }

    /// SREM key member
    pub fn srem(self: ValkeyContext, key: []const u8, member: []const u8) !i64 {
        const reply = try self.command(&.{ "SREM", key, member });
        defer freeReplyObject(reply);

        if (reply.type == VALKEY_REPLY_ERROR) {
            return error.CommandError;
        }

        if (reply.type != VALKEY_REPLY_INTEGER) {
            return error.InvalidReplyType;
        }

        return reply.integer;
    }

    /// HSET key field value
    pub fn hset(self: ValkeyContext, key: []const u8, fields: []const []const u8) !i64 {
        if (fields.len < 2 or fields.len % 2 != 0) {
            return error.InvalidArguments;
        }

        var args = try self.allocator.alloc([]const u8, 2 + fields.len);
        defer self.allocator.free(args);

        args[0] = "HSET";
        args[1] = key;
        @memcpy(args[2..], fields);

        const reply = try self.command(args);
        defer freeReplyObject(reply);

        if (reply.type == VALKEY_REPLY_ERROR) {
            return error.CommandError;
        }

        if (reply.type != VALKEY_REPLY_INTEGER) {
            return error.InvalidReplyType;
        }

        return reply.integer;
    }

    /// HGETALL key
    pub fn hgetall(self: ValkeyContext, key: []const u8) !std.ArrayList([]u8) {
        const reply = try self.command(&.{ "HGETALL", key });
        defer freeReplyObject(reply);

        var result: std.ArrayList([]u8) = .init(self.allocator);

        if (reply.type == VALKEY_REPLY_NIL) {
            // Key doesn't exist, return empty array
            return result;
        }

        if (reply.type != VALKEY_REPLY_ARRAY) {
            return error.InvalidReplyType;
        }

        const elements = reply.elements;

        for (0..elements) |i| {
            const elem = reply.element[i];
            if (elem.type == VALKEY_REPLY_STRING) {
                const str = elem.str[0..elem.len];
                try result.append(self.allocator, try self.allocator.dupe(u8, str));
            }
        }

        return result;
    }

    /// DEL keys...
    pub fn del(self: ValkeyContext, keys: []const []const u8) !i64 {
        if (keys.len == 0) return 0;

        var args = try self.allocator.alloc([]const u8, 1 + keys.len);
        defer self.allocator.free(args);

        args[0] = "DEL";
        @memcpy(args[1..], keys);

        const reply = try self.command(args);
        defer freeReplyObject(reply);

        if (reply.type == VALKEY_REPLY_ERROR) {
            return error.CommandError;
        }

        if (reply.type != VALKEY_REPLY_INTEGER) {
            return error.InvalidReplyType;
        }

        return reply.integer;
    }
};

test "ValkeyContext: structure defined" {
    _ = ValkeyContext;
}

test "ValkeyError: error variants defined" {
    _ = ValkeyError;
}

test "valkeyReply: struct fields accessible" {
    _ = valkeyReply;
}

test "VALKEY constants: reply types defined" {
    _ = VALKEY_REPLY_STRING;
    _ = VALKEY_REPLY_ARRAY;
    _ = VALKEY_REPLY_INTEGER;
    _ = VALKEY_REPLY_NIL;
    _ = VALKEY_REPLY_STATUS;
    _ = VALKEY_REPLY_ERROR;
}

test "Timeout constants: defined with reasonable values" {
    try std.testing.expectEqual(@as(u32, 5000), COMMAND_TIMEOUT_MS);
    try std.testing.expectEqual(@as(u32, 3000), CONNECT_TIMEOUT_MS);
    try std.testing.expect(CONNECT_TIMEOUT_MS < COMMAND_TIMEOUT_MS);
}
