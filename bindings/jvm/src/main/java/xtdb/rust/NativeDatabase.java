package xtdb.rust;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;

/** A native database region. The supplied Arrow allocator remains caller-owned until all imported batches close. */
public final class NativeDatabase extends NativeResource implements AdbcDatabase {
    private final BufferAllocator allocator;
    private final java.util.concurrent.atomic.AtomicReference<String> awaitToken = new java.util.concurrent.atomic.AtomicReference<>();
    private NativeDatabase(NativeAbi abi, long handle, BufferAllocator allocator) { super(abi, handle); this.allocator = allocator; }
    /** Honors native node YAML, including listeners; use server/flightSql null for a port-free embedded node. */
    public static NativeDatabase open(BufferAllocator allocator, String yaml, long nativeMemoryBytes) throws AdbcException {
        if (nativeMemoryBytes <= 0) throw AdbcException.invalidArgument("Native memory limit must be positive");
        NativeAbi abi = NativeAbi.get();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment text = NativeAbi.text(arena, yaml);
            return new NativeDatabase(abi, abi.handle("database_open", text, text.byteSize(), nativeMemoryBytes), allocator);
        }
    }
    @Override public NativeConnection connect() throws AdbcException { return connect("xtdb"); }
    public String getAwaitToken() { return awaitToken.get(); }
    /** Publishes next only when the stored token is the same object as previous, including null. */
    public boolean compareAndSetAwaitToken(String previous, String next) { return awaitToken.compareAndSet(previous, next); }
    public NativeConnection connect(String database) throws AdbcException { return connect(database, 0, "", ""); }
    /** Explicit host trust; this bypasses credential verification. */
    public NativeConnection connectTrusted(String database, String user) throws AdbcException { return connect(database, 1, user, ""); }
    public NativeConnection connectWithPassword(String database, String user, String password) throws AdbcException { return connect(database, 2, user, password); }
    public NativeConnection connectWithClientCredentials(String database, String clientId, String clientSecret) throws AdbcException { return connect(database, 3, clientId, clientSecret); }
    private NativeConnection connect(String database, int mode, String user, String secret) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment name = NativeAbi.text(arena, database), username = NativeAbi.text(arena, user), password = NativeAbi.text(arena, secret);
            long connection = abi.handle("session_open", handle(), name, name.byteSize(), mode, username, username.byteSize(), password, password.byteSize());
            return new NativeConnection(abi, connection, allocator, database);
        }
    }
    public long nativeMemoryUsed() throws AdbcException {
        try (Arena arena = Arena.ofConfined()) { MemorySegment out = arena.allocate(NativeAbi.I64); abi.call("memory_used", handle(), out); return out.get(NativeAbi.I64, 0); }
    }
}
