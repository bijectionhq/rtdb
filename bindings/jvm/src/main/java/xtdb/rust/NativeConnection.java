package xtdb.rust;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/** Calls share the native session's transaction state; serialize ordinary connection use. Cancellation is concurrent. */
public final class NativeConnection extends NativeResource implements AdbcConnection {
    final BufferAllocator allocator;
    private final Set<Operation> operations = ConcurrentHashMap.newKeySet();
    private String catalog;
    private boolean autoCommit = true;

    NativeConnection(NativeAbi abi, long handle, BufferAllocator allocator, String catalog) {
        super(abi, handle); this.allocator = allocator; this.catalog = catalog;
    }
    final class Operation implements AutoCloseable {
        private final AtomicLong id;
        Operation(long id) { this.id = new AtomicLong(id); operations.add(this); }
        long id() throws AdbcException { long value = id.get(); if (value == 0) throw AdbcException.invalidState("Operation is closed"); return value; }
        void cancel() throws AdbcException { long value = id.get(); if (value != 0) { synchronized (this) { if (id.get() != 0) abi.call("cancel", value); } } }
        @Override public synchronized void close() throws AdbcException { operations.remove(this); abi.dispose(id.getAndSet(0), false); }
    }
    Operation operation() throws AdbcException { return new Operation(abi.handle("operation_new", handle())); }
    @Override public void cancel() throws AdbcException {
        AdbcException first = null;
        for (Operation operation : operations) { try { operation.cancel(); } catch (AdbcException error) { if (first == null) first = error; else first.addSuppressed(error); } }
        if (first != null) throw first;
    }
    @Override public NativeStatement createStatement() throws AdbcException { return createStatement(""); }
    public NativeStatement createStatement(String sql) throws AdbcException { return statement(sql, false); }
    @Override public NativeStatement bulkIngest(String table, BulkIngestMode mode) throws AdbcException {
        if (mode != BulkIngestMode.CREATE_APPEND) throw AdbcException.invalidArgument("Only CREATE_APPEND mode is supported");
        return statement(table, true);
    }
    private NativeStatement statement(String source, boolean bulk) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment text = NativeAbi.text(arena, source);
            return new NativeStatement(this, abi.handle("statement_new", handle(), text, text.byteSize(), bulk ? 1 : 0));
        }
    }
    private void action(int action) throws AdbcException { try (Operation operation = operation()) { abi.call("session_action", handle(), operation.id(), action); } }
    @Override public boolean getAutoCommit() throws AdbcException { handle(); return autoCommit; }
    @Override public void setAutoCommit(boolean enabled) throws AdbcException { action(enabled ? 1 : 0); autoCommit = enabled; }
    @Override public void commit() throws AdbcException { action(3); }
    @Override public void rollback() throws AdbcException { action(4); }
    public void begin() throws AdbcException { action(2); }
    public void commitSync() throws AdbcException { action(5); }
    public void commitAsync() throws AdbcException { action(6); }
    public void rollbackTx() throws AdbcException { action(7); }
    public String getAwaitToken() throws AdbcException {
        long token = abi.handle("session_await_token", handle());
        if (token == 0) return null;
        try { return new String(abi.copiedBytes(token), java.nio.charset.StandardCharsets.UTF_8); } finally { abi.dispose(token, false); }
    }
    /** Native transaction request encoded by xtdb.rust; returns the transaction key as Transit JSON. */
    public byte[] transact(byte[] request) throws AdbcException {
        try (Operation operation = operation(); Arena arena = Arena.ofConfined()) {
            MemorySegment input = NativeAbi.bytes(arena, request);
            long result = abi.handle("session_transact", handle(), operation.id(), input, input.byteSize());
            try { return abi.copiedBytes(result); } finally { abi.dispose(result, false); }
        }
    }
    public NativeRows queryLogicalRelation(String plan, byte[] arguments) throws AdbcException {
        Operation operation = operation();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment source = NativeAbi.text(arena, plan);
            MemorySegment bindings = NativeAbi.bytes(arena, arguments);
            long cursor = abi.handle("session_query_ra", handle(), operation.id(), source, source.byteSize(), bindings, bindings.byteSize());
            return new NativeRows(this, cursor, operation);
        } catch (Throwable error) { try { operation.close(); } catch (AdbcException close) { error.addSuppressed(close); } throw error; }
    }
    @Override public String getCurrentCatalog() throws AdbcException { handle(); return catalog; }
    @Override public String getCurrentDbSchema() { return "public"; }
    @Override public void setCurrentCatalog(String catalog) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) { MemorySegment text = NativeAbi.text(arena, catalog); abi.call("session_set_catalog", handle(), text, text.byteSize()); this.catalog = catalog; }
    }
    public void setApplicationName(String name) throws AdbcException {
        if (!abi.supports("session_set_application_name")) {
            String literal = name.replace("\\", "\\\\").replace("'", "''");
            try (var statement = createStatement("SET application_name = E'" + literal + "'")) {
                statement.executeUpdate();
            }
            return;
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment text = NativeAbi.text(arena, name);
            abi.call("session_set_application_name", handle(), text, text.byteSize());
        }
    }
    @Override public ArrowReader getInfo(int[] codes) throws AdbcException {
        String values = codes == null ? "null" : Arrays.stream(codes).mapToObj(Integer::toUnsignedString).collect(Collectors.joining(",", "[", "]"));
        return metadata("{\"kind\":0,\"info_codes\":" + values + "}");
    }
    @Override public ArrowReader getTableTypes() throws AdbcException { return metadata("{\"kind\":1}"); }
    @Override public ArrowReader getObjects(GetObjectsDepth depth, String catalogPattern, String schemaPattern, String tablePattern, String[] tableTypes, String columnPattern) throws AdbcException {
        String types = tableTypes == null ? "null" : Arrays.stream(tableTypes).map(NativeAbi::json).collect(Collectors.joining(",", "[", "]"));
        return metadata("{\"kind\":2,\"depth\":" + depth.ordinal() + ",\"catalog\":" + NativeAbi.json(catalogPattern) + ",\"schema\":" + NativeAbi.json(schemaPattern) + ",\"table\":" + NativeAbi.json(tablePattern) + ",\"column\":" + NativeAbi.json(columnPattern) + ",\"table_types\":" + types + "}");
    }
    @Override public Schema getTableSchema(String catalog, String schema, String table) throws AdbcException {
        try (NativeArrowReader reader = metadata("{\"kind\":3,\"catalog\":" + NativeAbi.json(catalog) + ",\"schema\":" + NativeAbi.json(schema) + ",\"table\":" + NativeAbi.json(table) + "}")) {
            return reader.getVectorSchemaRoot().getSchema();
        } catch (java.io.IOException error) { throw AdbcException.io(error.getMessage()).withCause(error); }
    }
    private NativeArrowReader metadata(String request) throws AdbcException {
        Operation operation = operation();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment text = NativeAbi.text(arena, request);
            long cursor = abi.handle("metadata", handle(), operation.id(), text, text.byteSize());
            return new NativeArrowReader(this, cursor, operation);
        } catch (Throwable error) { try { operation.close(); } catch (AdbcException close) { error.addSuppressed(close); } throw error; }
    }
}
