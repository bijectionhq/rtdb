package xtdb.rust;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;

final class NativeAbi {
    static final ValueLayout.OfLong I64 = ValueLayout.JAVA_LONG;
    static final ValueLayout.OfInt I32 = ValueLayout.JAVA_INT;
    static final java.lang.foreign.AddressLayout PTR = ValueLayout.ADDRESS;
    private final Map<String, MethodHandle> functions = new HashMap<>();

    private NativeAbi() {
        if (PTR.byteSize() != 8) throw new IllegalStateException("RTDB v1 JVM bindings require a 64-bit process");
        String path = System.getProperty("xtdb.native.library");
        if (path == null) throw new IllegalStateException("Set xtdb.native.library to the absolute rtdb_ffi shared-library path");
        SymbolLookup library = SymbolLookup.libraryLookup(Path.of(path), Arena.global());
        Linker linker = Linker.nativeLinker();
        register(library, linker, "abi_version");
        register(library, linker, "database_open", PTR, I64, I64, PTR, PTR);
        register(library, linker, "database_open_with_properties", PTR, I64, I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "database_open_with_log_clock", PTR, I64, I64, PTR, I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "database_flush_all", I64, I64, PTR);
        registerOptional(library, linker, "database_sync_all", I64, I64, PTR);
        register(library, linker, "retain", I64, PTR, PTR);
        register(library, linker, "release", I64, PTR);
        register(library, linker, "close", I64, PTR);
        register(library, linker, "error_field", I64, I32, PTR, PTR);
        register(library, linker, "session_open", I64, PTR, I64, I32, PTR, I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "database_authenticate", I64, I32, PTR, I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "principal_user_id", I64, PTR, PTR);
        registerOptional(library, linker, "session_open_as", I64, PTR, I64, I64, PTR, PTR);
        registerOptional(library, linker, "session_user_id", I64, PTR, PTR);
        registerOptional(library, linker, "database_start_device_auth", I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "device_auth_url", I64, PTR, PTR);
        registerOptional(library, linker, "device_auth_wait", I64, PTR, PTR);
        register(library, linker, "operation_new", I64, PTR, PTR);
        register(library, linker, "cancel", I64, PTR);
        register(library, linker, "session_action", I64, I64, I32, PTR);
        registerOptional(library, linker, "session_commit_result", I64, I64, I32, PTR, PTR);
        register(library, linker, "session_set_catalog", I64, PTR, I64, PTR);
        registerOptional(library, linker, "session_set_application_name", I64, PTR, I64, PTR);
        register(library, linker, "session_transact", I64, I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "session_transact_result", I64, I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "session_query_ra", I64, I64, PTR, I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "session_query_ra_arrow", I64, I64, PTR, I64, PTR, PTR, PTR, PTR);
        register(library, linker, "session_await_token", I64, PTR, PTR);
        registerOptional(library, linker, "session_set_await_token", I64, I64, PTR, I64, PTR);
        registerOptional(library, linker, "session_set_read_settings", I64, I64, PTR, I64, PTR, I64, PTR, PTR, PTR);
        registerOptional(library, linker, "session_begin_read_only", I64, I64, PTR, I64, PTR, I64, PTR, PTR, PTR);
        registerOptional(library, linker, "session_begin_write_only", I64, I64, PTR, I64, PTR, PTR, I64, I32, PTR);
        registerOptional(library, linker, "session_set_transaction_read_only", I64, I64, PTR, I64, PTR, PTR, PTR, I64, PTR);
        registerOptional(library, linker, "session_set_transaction_read_write", I64, I64, PTR, PTR, I64, I32, PTR);
        registerOptional(library, linker, "session_set_transaction_timezone", I64, I64, PTR, I64, PTR);
        register(library, linker, "statement_new", I64, PTR, I64, I32, PTR, PTR);
        register(library, linker, "statement_set_sql", I64, PTR, I64, PTR);
        register(library, linker, "statement_prepare", I64, I64, PTR);
        register(library, linker, "statement_bind", I64, PTR, PTR, PTR);
        registerOptional(library, linker, "statement_bind_retained", I64, PTR, PTR, PTR);
        register(library, linker, "statement_bind_transit", I64, PTR, I64, PTR);
        register(library, linker, "statement_schema", I64, I64, I32, PTR, PTR);
        registerOptional(library, linker, "statement_schema_with_parameters", I64, I64, PTR, PTR, PTR);
        register(library, linker, "statement_query", I64, I64, PTR, PTR);
        register(library, linker, "statement_update", I64, I64, PTR, PTR);
        register(library, linker, "cursor_schema", I64, PTR, PTR);
        register(library, linker, "cursor_next", I64, PTR, PTR, PTR, PTR);
        registerOptional(library, linker, "cursor_next_array", I64, PTR, PTR, PTR);
        registerOptional(library, linker, "cursor_next_row_bytes", I64, PTR, PTR, PTR);
        register(library, linker, "cursor_consumer_error", I64, PTR);
        register(library, linker, "bytes", I64, PTR, PTR);
        register(library, linker, "metadata", I64, I64, PTR, I64, PTR, PTR);
        register(library, linker, "memory_used", I64, PTR, PTR);
        register(library, linker, "database_pgwire_port", I64, PTR, PTR);
        register(library, linker, "database_pgwire_client_host", I64, PTR, I64, PTR, PTR);
        registerOptional(library, linker, "database_read_only_pgwire_port", I64, PTR, PTR);
        registerOptional(library, linker, "database_read_only_pgwire_client_host", I64, PTR, I64, PTR, PTR);
        register(library, linker, "database_flight_sql_port", I64, PTR, PTR);
        registerOptional(library, linker, "database_healthz_port", I64, PTR, PTR);
        registerOptional(library, linker, "database_catalog_status", I64, PTR, PTR);
        registerOptional(library, linker, "database_auth_method", I64, PTR, I64, PTR, I64, PTR, PTR);
        try { if (invoke("abi_version") != 1) throw new IllegalStateException("Unsupported RTDB native ABI version"); }
        catch (AdbcException error) { throw new IllegalStateException(error); }
    }
    private static final class Holder { static final NativeAbi INSTANCE = new NativeAbi(); }
    static NativeAbi get() { return Holder.INSTANCE; }
    private void register(SymbolLookup library, Linker linker, String name, MemoryLayout... arguments) {
        functions.put(name, linker.downcallHandle(library.find("rtdb_v1_" + name).orElseThrow(), FunctionDescriptor.of(I32, arguments)));
    }
    private void registerOptional(SymbolLookup library, Linker linker, String name, MemoryLayout... arguments) {
        library.find("rtdb_v1_" + name).ifPresent(symbol -> functions.put(name, linker.downcallHandle(symbol, FunctionDescriptor.of(I32, arguments))));
    }
    boolean supports(String name) { return functions.containsKey(name); }
    private int invoke(String name, Object... arguments) throws AdbcException {
        MethodHandle function = functions.get(name);
        if (function == null) throw AdbcException.invalidState("Native ABI entry point is unavailable: " + name);
        try { return (int) function.invokeWithArguments(arguments); }
        catch (Throwable error) { throw AdbcException.internal("Native ABI invocation failed: " + name).withCause(error); }
    }
    void call(String name, Object... arguments) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment error = arena.allocate(I64);
            Object[] full = Arrays.copyOf(arguments, arguments.length + 1);
            full[arguments.length] = error;
            if (invoke(name, full) != 0) throw diagnostic(error.get(I64, 0));
        }
    }
    long handle(String name, Object... arguments) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(I64);
            Object[] full = Arrays.copyOf(arguments, arguments.length + 1);
            full[arguments.length] = out;
            call(name, full);
            return out.get(I64, 0);
        }
    }
    void dispose(long handle, boolean close) throws AdbcException {
        if (handle == 0) return;
        AdbcException failure = null;
        try { if (close) call("close", handle); } catch (AdbcException error) { failure = error; }
        try { call("release", handle); } catch (AdbcException error) { if (failure == null) failure = error; else failure.addSuppressed(error); }
        if (failure != null) throw failure;
    }
    byte[] copiedBytes(long handle) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment slice = arena.allocate(16, 8);
            call("bytes", handle, slice);
            return copySlice(slice);
        }
    }
    String ownedText(long handle) throws AdbcException {
        if (handle == 0) return null;
        try { return new String(copiedBytes(handle), StandardCharsets.UTF_8); }
        finally { dispose(handle, false); }
    }
    NativeException diagnostic(long handle) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            byte[][] fields = new byte[4][];
            String message = null;
            for (int field = 0; field < fields.length; field++) {
                MemorySegment slice = arena.allocate(16, 8);
                MemorySegment error = arena.allocate(I64);
                if (invoke("error_field", handle, field, slice, error) != 0) {
                    long secondary = error.get(I64, 0);
                    MemorySegment ignored = arena.allocate(I64);
                    invoke("release", secondary, ignored);
                    throw AdbcException.internal("Unable to read native diagnostic");
                }
                if (field == 2) message = diagnosticMessage(slice);
                else fields[field] = copySlice(slice);
            }
            return new NativeException(new String(fields[0], StandardCharsets.UTF_8), new String(fields[1], StandardCharsets.UTF_8), message, fields[3]);
        } finally { dispose(handle, false); }
    }
    static String diagnosticMessage(MemorySegment slice) throws AdbcException {
        if (slice.get(PTR, 0).address() == 0) {
            if (slice.get(I64, 8) != 0) throw AdbcException.internal("Absent native diagnostic message has a nonzero length");
            return null;
        }
        return new String(copySlice(slice), StandardCharsets.UTF_8);
    }
    private static byte[] copySlice(MemorySegment slice) {
        long length = slice.get(I64, 8);
        if (length == 0) return new byte[0];
        return slice.get(PTR, 0).reinterpret(length).toArray(ValueLayout.JAVA_BYTE);
    }
    static MemorySegment bytes(Arena arena, byte[] bytes) { return arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes); }
    static MemorySegment text(Arena arena, String text) { return bytes(arena, text.getBytes(StandardCharsets.UTF_8)); }
    static MemorySegment optionalText(Arena arena, String value) {
        if (value == null) return MemorySegment.NULL;
        return value.isEmpty() ? arena.allocate(1).asSlice(0, 0) : text(arena, value);
    }
    static MemorySegment address(long address) { return MemorySegment.ofAddress(address); }
    static String json(String text) {
        if (text == null) return "null";
        StringBuilder out = new StringBuilder("\"");
        for (char ch : text.toCharArray()) {
            switch (ch) { case '"' -> out.append("\\\""); case '\\' -> out.append("\\\\"); case '\n' -> out.append("\\n"); case '\r' -> out.append("\\r"); case '\t' -> out.append("\\t"); default -> { if (ch < 32) out.append(String.format("\\u%04x", (int) ch)); else out.append(ch); } }
        }
        return out.append('"').toString();
    }
}
