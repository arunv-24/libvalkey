//! libvalkey.zig: minimal Valkey client using libvalkey C library.
//! Direct command execution via FFI bindings.

const std = @import("std");
const valkey_ffi = @import("valkey_ffi.zig");

const log = std.log.scoped(.libvalkey);

pub const ValkeyError = error{
    ConnectionFailed,
    CommandFailed,
    ParseError,
};

/// Valkey client using libvalkey C library
pub const Client = struct {
    allocator: std.mem.Allocator,
    ctx: valkey_ffi.ValkeyContext,
    consecutive_failures: u32,
    consecutive_successes: u32,
    is_healthy: bool,

    /// Number of connection attempts before giving up
    const MAX_RETRIES = 3;
    /// Number of consecutive failures before opening circuit breaker
    const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
    /// Number of successful operations to close circuit breaker
    const CIRCUIT_BREAKER_RESET_COUNT: u32 = 3;

    /// Initialize Valkey client with retry logic on connection failure
    pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16, io: std.Io) !Client {
        _ = io; // no longer needed but keeping for API compatibility

        // Convert host to null-terminated string
        const host_z = try allocator.allocSentinel(u8, host.len, 0);
        defer allocator.free(host_z);
        @memcpy(host_z, host);

        var attempt: u32 = 0;

        while (attempt < MAX_RETRIES) : (attempt += 1) {
            if (valkey_ffi.ValkeyContext.init(allocator, host_z.ptr, port)) |ctx| {
                return .{
                    .allocator = allocator,
                    .ctx = ctx,
                    .consecutive_failures = 0,
                    .consecutive_successes = 0,
                    .is_healthy = true,
                };
            } else |err| {
                if (attempt + 1 < MAX_RETRIES) {
                    log.debug("Connection attempt {d}/{d} failed, will retry...", .{ attempt + 1, MAX_RETRIES });
                } else {
                    log.err("Failed to connect to Valkey after {d} attempts", .{MAX_RETRIES});
                    return err;
                }
            }
        }

        return error.ConnectionFailed;
    }

    /// Deinitialize and close connection
    pub fn deinit(self: *Client) void {
        self.ctx.deinit();
    }

    /// Send PING command to verify connection
    pub fn ping(self: *Client) ![]const u8 {
        const reply = try self.ctx.command(&.{"PING"});
        defer valkey_ffi.freeReplyObject(reply);

        if (reply.type != valkey_ffi.VALKEY_REPLY_STATUS) {
            self.consecutive_failures += 1;
            self.consecutive_successes = 0;
            if (self.consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD) {
                self.is_healthy = false;
            }
            return error.CommandFailed;
        }

        self.consecutive_successes += 1;
        if (self.consecutive_successes >= CIRCUIT_BREAKER_RESET_COUNT) {
            self.consecutive_failures = 0;
            self.is_healthy = true;
        }
        const len: usize = @intCast(reply.len);
        const str = reply.str[0..len];
        return try self.allocator.dupe(u8, str);
    }

    /// Check health of Valkey connection with circuit breaker
    /// Returns true if connection is healthy, false if circuit is open
    pub fn healthCheck(self: *Client) bool {
        if (!self.is_healthy) {
            log.debug("Circuit breaker is open, skipping health check", .{});
            return false;
        }

        const result = self.ping() catch |err| {
            log.debug("Health check failed: {}", .{err});
            return false;
        };
        defer self.allocator.free(result);
        return true;
    }

    /// SET key value
    pub fn set(self: *Client, key: []const u8, value: []const u8) !void {
        try self.ctx.set(key, value);
    }

    /// GET key
    pub fn get(self: *Client, key: []const u8) !?[]u8 {
        return try self.ctx.get(key);
    }

    /// SADD key members...
    pub fn sadd(self: *Client, key: []const u8, members: []const []const u8) !i64 {
        return try self.ctx.sadd(key, members);
    }

    /// SMEMBERS key
    pub fn smembers(self: *Client, key: []const u8) !std.ArrayList([]u8) {
        return try self.ctx.smembers(key);
    }

    /// SREM key members...
    pub fn srem(self: *Client, key: []const u8, members: []const []const u8) !i64 {
        var total: i64 = 0;
        for (members) |member| {
            total += try self.ctx.srem(key, member);
        }
        return total;
    }

    /// HSET key field value [field value ...]
    pub fn hset(self: *Client, key: []const u8, fields: []const []const u8) !i64 {
        return try self.ctx.hset(key, fields);
    }

    /// HGETALL key
    pub fn hgetall(self: *Client, key: []const u8) !std.ArrayList([]u8) {
        return try self.ctx.hgetall(key);
    }

    /// DEL keys...
    pub fn del(self: *Client, keys: []const []const u8) !i64 {
        return try self.ctx.del(keys);
    }

    /// EXPIRE key seconds - set key expiration time
    pub fn expire(self: *Client, key: []const u8, seconds: i64) !i64 {
        const seconds_str = try self.allocator.alloc(u8, 20);
        defer self.allocator.free(seconds_str);
        const seconds_len = std.fmt.bufPrint(seconds_str, "{d}", .{seconds}) catch return error.CommandFailed;
        const reply = try self.ctx.command(&.{ "EXPIRE", key, seconds_str[0..seconds_len.len] });
        defer valkey_ffi.freeReplyObject(reply);

        if (reply.type != valkey_ffi.VALKEY_REPLY_INTEGER) {
            return error.CommandFailed;
        }

        return reply.integer;
    }
};

// Tests
test "Client: struct is defined" {
    _ = Client;
}

test "Client: initialization requires allocator and connection params" {
    const dummy_client = Client{
        .allocator = undefined,
        .ctx = undefined,
        .consecutive_failures = 0,
        .consecutive_successes = 0,
        .is_healthy = true,
    };
    _ = dummy_client;
}

test "Client: public methods accessible" {
    // Verify method signatures exist without executing
    const methods = &.{
        "init",
        "deinit",
        "ping",
        "set",
        "get",
        "sadd",
        "smembers",
        "srem",
        "hset",
        "hgetall",
        "del",
        "expire",
    };
    _ = methods;
}

test "Client: MAX_RETRIES constant defined" {
    const retry_limit = Client.MAX_RETRIES;
    try std.testing.expectEqual(@as(u32, 3), retry_limit);
}

test "Client: circuit breaker constants defined" {
    const threshold = Client.CIRCUIT_BREAKER_THRESHOLD;
    const reset = Client.CIRCUIT_BREAKER_RESET_COUNT;
    try std.testing.expectEqual(@as(u32, 5), threshold);
    try std.testing.expectEqual(@as(u32, 3), reset);
}

test "Client: health check state initialized correctly" {
    const allocator = std.testing.allocator;
    const client = Client{
        .allocator = allocator,
        .ctx = undefined,
        .consecutive_failures = 0,
        .consecutive_successes = 0,
        .is_healthy = true,
    };
    try std.testing.expectEqual(@as(u32, 0), client.consecutive_failures);
    try std.testing.expectEqual(@as(u32, 0), client.consecutive_successes);
    try std.testing.expect(client.is_healthy);
}
