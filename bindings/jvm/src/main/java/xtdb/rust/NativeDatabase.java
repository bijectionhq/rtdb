package xtdb.rust;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;

/** A native database region. The supplied Arrow allocator remains caller-owned until all imported batches close. */
public final class NativeDatabase extends NativeResource implements AdbcDatabase, xtdb.api.DataSource {
    private final BufferAllocator allocator;
    private final java.util.concurrent.atomic.AtomicReference<String> awaitToken = new java.util.concurrent.atomic.AtomicReference<>();
    private final Arena clockArena;
    private NativeDatabase(NativeAbi abi, AtomicLong handle, BufferAllocator allocator, Arena clockArena) {
        super(abi, handle);
        this.allocator = allocator;
        this.clockArena = clockArena;
    }
    private static final class SortProperty {
        static final boolean LEGACY = Boolean.getBoolean("java.util.Arrays.useLegacyMergeSort");
    }
    /** Opens native node YAML, including listeners; use server/flightSql null for a port-free node.
     * The first call captures java.util.Arrays.useLegacyMergeSort; subsequent calls reuse that selection.
     * This capture cannot recover an earlier value already cached by the JVM's Arrays sorter.
     * The allocator remains caller-owned; invalid configuration and unavailable resources throw AdbcException.
     * nativeMemoryBytes must be positive and bounds native runtime allocations in bytes.
     */
    public static NativeDatabase open(BufferAllocator allocator, String yaml, long nativeMemoryBytes) throws AdbcException {
        return open(allocator, yaml, nativeMemoryBytes, SortProperty.LEGACY);
    }
    /** Uses an explicitly captured object-sort selection for this native runtime.
     * The allocator remains caller-owned; YAML and property bytes are copied before return.
     * Invalid configuration or unavailable native resources throw AdbcException.
     * nativeMemoryBytes must be positive and bounds native runtime allocations in bytes.
     */
    public static NativeDatabase open(BufferAllocator allocator, String yaml, long nativeMemoryBytes, boolean legacyMergeSort) throws AdbcException {
        if (nativeMemoryBytes <= 0) throw AdbcException.invalidArgument("Native memory limit must be positive");
        NativeAbi abi = NativeAbi.get();
        var handle = new AtomicLong();
        var database = new NativeDatabase(abi, handle, allocator, null);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment text = NativeAbi.text(arena, yaml);
            MemorySegment properties = sortProperties(arena, legacyMergeSort);
            handle.set(abi.handle("database_open_with_properties", text, text.byteSize(), nativeMemoryBytes, properties, properties.byteSize()));
        }
        return database;
    }
    /** Supplies timestamps for transactions appended to in-memory and local source logs.
     * The clock can be called on native threads, concurrently across logs, and must never reenter the database.
     * Clock failures reject the append. The database retains the clock until it closes.
     */
    public static NativeDatabase open(BufferAllocator allocator, String yaml, long nativeMemoryBytes, InstantSource logClock) throws AdbcException {
        if (nativeMemoryBytes <= 0) throw AdbcException.invalidArgument("Native memory limit must be positive");
        if (logClock == null) throw AdbcException.invalidArgument("Log clock must be supplied");
        NativeAbi abi = NativeAbi.get();
        Arena clockArena = Arena.ofShared();
        try (Arena arena = Arena.ofConfined()) {
            var handle = new AtomicLong();
            var database = new NativeDatabase(abi, handle, allocator, clockArena);
            MemorySegment text = NativeAbi.text(arena, yaml);
            MemorySegment properties = sortProperties(arena, SortProperty.LEGACY);
            MemorySegment clock = LogClock.upcall(logClock, clockArena);
            handle.set(abi.handle("database_open_with_log_clock", text, text.byteSize(), nativeMemoryBytes,
                properties, properties.byteSize(), clock, 0L));
            return database;
        } catch (AdbcException | RuntimeException | Error failure) {
            clockArena.close();
            throw failure;
        }
    }

    private static MemorySegment sortProperties(Arena arena, boolean legacyMergeSort) {
        return NativeAbi.text(arena, "{\"java.util.Arrays.useLegacyMergeSort\":\"" + legacyMergeSort + "\"}");
    }

    private static final class LogClock {
        private static final FunctionDescriptor ABI = FunctionDescriptor.of(NativeAbi.I32, NativeAbi.I64, NativeAbi.PTR, NativeAbi.PTR);
        private static final MethodHandle READ = readHandle();

        static MemorySegment upcall(InstantSource clock, Arena arena) {
            return Linker.nativeLinker().upcallStub(READ.bindTo(clock), ABI, arena);
        }

        private static MethodHandle readHandle() {
            try {
                return MethodHandles.lookup().findStatic(LogClock.class, "read",
                    MethodType.methodType(int.class, InstantSource.class, long.class, MemorySegment.class, MemorySegment.class));
            } catch (ReflectiveOperationException invalidMethod) {
                throw new ExceptionInInitializerError(invalidMethod);
            }
        }

        private static int read(InstantSource clock, long context, MemorySegment seconds, MemorySegment nanos) {
            try {
                Instant instant = clock.instant();
                seconds.reinterpret(NativeAbi.I64.byteSize()).set(NativeAbi.I64, 0, instant.getEpochSecond());
                nanos.reinterpret(NativeAbi.I32.byteSize()).set(NativeAbi.I32, 0, instant.getNano());
                return 0;
            } catch (Throwable failure) {
                return 1;
            }
        }
    }

    @Override public synchronized void close() throws AdbcException {
        try {
            super.close();
        } finally {
            if (clockArena != null && clockArena.scope().isAlive()) clockArena.close();
        }
    }

    /** Requests a block flush in each open writable database and awaits progress through those requests. */
    public void flushAll(Duration timeout) throws AdbcException {
        if (timeout == null || timeout.isZero() || timeout.isNegative())
            throw AdbcException.invalidArgument("Flush timeout must be positive");
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException tooLarge) {
            throw AdbcException.invalidArgument("Flush timeout exceeds the signed nanosecond range").withCause(tooLarge);
        }
        abi.call("database_flush_all", handle(), nanos);
    }

    /** Awaits indexing of every message each open database's source log held when the call began. */
    public void syncAll(Duration timeout) throws AdbcException {
        if (timeout == null || timeout.isZero() || timeout.isNegative())
            throw AdbcException.invalidArgument("Sync timeout must be positive");
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException tooLarge) {
            throw AdbcException.invalidArgument("Sync timeout exceeds the signed nanosecond range").withCause(tooLarge);
        }
        abi.call("database_sync_all", handle(), nanos);
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
    /** The bound PostgreSQL wire port, or -1 when the configuration starts no pgwire listener. */
    public int getServerPort() throws AdbcException {
        try (Arena arena = Arena.ofConfined()) { MemorySegment out = arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT); abi.call("database_pgwire_port", handle(), out); return out.get(java.lang.foreign.ValueLayout.JAVA_INT, 0); }
    }
    /** The host a client reaches the pgwire listener on: the bound address, or localhost for a wildcard bind. */
    private String getServerClientHost() throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(256);
            MemorySegment length = arena.allocate(java.lang.foreign.ValueLayout.JAVA_LONG);
            abi.call("database_pgwire_client_host", handle(), out, 256L, length);
            return new String(out.asSlice(0, length.get(java.lang.foreign.ValueLayout.JAVA_LONG, 0)).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
    /** The bound Flight SQL port, or -1 when the configuration starts no Flight SQL listener. */
    public int getFlightSqlPort() throws AdbcException {
        try (Arena arena = Arena.ofConfined()) { MemorySegment out = arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT); abi.call("database_flight_sql_port", handle(), out); return out.get(java.lang.foreign.ValueLayout.JAVA_INT, 0); }
    }
    @Override public void setAwaitToken(String token) { awaitToken.set(token); }
    /** JDBC connections go through this node's own pgwire listener, as for a JVM node. */
    @Override public xtdb.api.DataSource.ConnectionBuilder createConnectionBuilder() {
        int port;
        try { port = getServerPort(); } catch (AdbcException error) { throw new IllegalStateException(error); }
        if (port < 0) throw new IllegalStateException("This native node was opened without a pgwire server");
        String host;
        try { host = getServerClientHost(); } catch (AdbcException error) { throw new IllegalStateException(error); }
        return new xtdb.api.DataSource.ConnectionBuilder(host, port);
    }
    @Override public <T> T unwrap(Class<T> type) throws java.sql.SQLException {
        if (type.isInstance(this)) return type.cast(this);
        throw new java.sql.SQLException("Not a wrapper for " + type.getName());
    }
    @Override public boolean isWrapperFor(Class<?> type) { return type.isInstance(this); }
    public long nativeMemoryUsed() throws AdbcException {
        try (Arena arena = Arena.ofConfined()) { MemorySegment out = arena.allocate(NativeAbi.I64); abi.call("memory_used", handle(), out); return out.get(NativeAbi.I64, 0); }
    }
}
