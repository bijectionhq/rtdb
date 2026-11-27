package xtdb.rust;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import xtdb.api.TableRef;

/** Calls share the native session's transaction state; serialize ordinary connection use. Cancellation is concurrent. */
public final class NativeConnection extends NativeResource implements AdbcConnection {
    public record SubmittedTx(long txId) implements CommitOutcome {}
    public record ExecutedTx(long txId, Instant systemTime, boolean committed, NativeException error) implements CommitOutcome {}
    public sealed interface TxOperation permits SqlOperation, PutDocuments, PatchDocuments, DeleteDocuments, EraseDocuments {}
    /** A SQL transaction operation. Null arguments mean unbound SQL and an empty batch executes no rows. */
    public record SqlOperation(String sql, List<? extends List<?>> argumentRows) implements TxOperation {
        public SqlOperation(String sql) { this(sql, null); }
    }
    public record PutDocuments(TableRef table, List<? extends Map<?, ?>> documents, Instant validFrom, Instant validTo) implements TxOperation {}
    public record PatchDocuments(TableRef table, List<? extends Map<?, ?>> documents, Instant validFrom, Instant validTo) implements TxOperation {}
    public record DeleteDocuments(TableRef table, List<?> ids, Instant validFrom, Instant validTo) implements TxOperation {}
    public record EraseDocuments(TableRef table, List<?> ids) implements TxOperation {}
    public record TxOptions(ZoneId defaultTimezone, Instant systemTime, Map<?, ?> metadata) {}
    private sealed interface CommitOutcome permits SubmittedTx, ExecutedTx {}

    private static final MemoryLayout COMMIT_RESULT = MemoryLayout.structLayout(
        NativeAbi.I64, NativeAbi.I64, NativeAbi.I64, NativeAbi.I32, NativeAbi.I32);

    private static final class TransactionCodec {
        static final IFn WRITE = (IFn) Clojure.var("clojure.core", "requiring-resolve").invoke(Clojure.read("xtdb.rust/write-request"));
    }

    final BufferAllocator allocator;
    private final Set<Operation> operations = ConcurrentHashMap.newKeySet();
    private String catalog;
    private boolean autoCommit = true;

    NativeConnection(NativeAbi abi, long handle, BufferAllocator allocator, String catalog) {
        super(abi, handle); this.allocator = allocator; this.catalog = catalog;
    }
    public BufferAllocator getAllocator() { return allocator; }
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
    public NativeStatement prepareStatement(String sql) throws AdbcException {
        var statement = createStatement(sql);
        try {
            statement.prepare();
            return statement;
        } catch (AdbcException | RuntimeException | Error failure) {
            try { statement.close(); } catch (AdbcException close) { failure.addSuppressed(close); }
            throw failure;
        }
    }
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
    /** Opens an explicit read-only transaction at the connection's basis. */
    public void beginReadOnly() throws AdbcException { beginReadOnly(null, null, null, null); }
    /** Opens a read-only transaction with optional basis and time-zone overrides. */
    public void beginReadOnly(ZoneId timezone, String snapshotToken, Instant snapshotTime, Instant clockTime) throws AdbcException {
        changeReadSettings("session_begin_read_only", timezone == null ? null : timezone.getId(), snapshotToken, snapshotTime, clockTime, false);
    }
    /** Opens an explicit write-only transaction with the session time zone and synchronous ADBC commit. */
    public void beginWriteOnly() throws AdbcException { beginWriteOnly(null, null, false, null); }
    /** Opens a write-only transaction with optional backfill time, metadata, commit mode, and time zone. */
    public void beginWriteOnly(Instant systemTime, Map<?, ?> metadata, boolean asynchronous, ZoneId timezone) throws AdbcException {
        try (Operation operation = operation(); Arena arena = Arena.ofConfined()) {
            var zone = NativeAbi.optionalText(arena, timezone == null ? null : timezone.getId());
            var metadataBytes = metadata == null ? MemorySegment.NULL : NativeAbi.bytes(arena, (byte[]) TransactionCodec.WRITE.invoke(metadata));
            abi.call("session_begin_write_only", handle(), operation.id(), zone, zone.byteSize(), readTime(arena, systemTime, false),
                metadataBytes, metadataBytes.byteSize(), asynchronous ? 1 : 0);
        }
    }
    public void commitSync() throws AdbcException { action(5); }
    public void commitAsync() throws AdbcException { action(6); }
    /** Commits an explicit write transaction and returns its indexed outcome, including an abort. */
    public ExecutedTx commitSyncResult() throws AdbcException { return (ExecutedTx) commitResult(false); }
    /** Submits an explicit write transaction and returns its transaction ID. */
    public SubmittedTx commitAsyncResult() throws AdbcException { return (SubmittedTx) commitResult(true); }
    public void rollbackTx() throws AdbcException { action(7); }

    private CommitOutcome commitResult(boolean asynchronous) throws AdbcException {
        try (Operation operation = operation(); Arena arena = Arena.ofConfined()) {
            var out = arena.allocate(COMMIT_RESULT);
            abi.call("session_commit_result", handle(), operation.id(), asynchronous ? 1 : 0, out);
            return decodeCommitResult(out);
        }
    }

    private CommitOutcome decodeCommitResult(MemorySegment result) throws AdbcException {
        long txId = result.get(NativeAbi.I64, 0);
        long micros = result.get(NativeAbi.I64, 8);
        long diagnostic = result.get(NativeAbi.I64, 16);
        int kind = result.get(NativeAbi.I32, 24);
        NativeException abort = diagnostic == 0 ? null : abi.diagnostic(diagnostic);
        return switch (kind) {
            case 0 -> null;
            case 1 -> new SubmittedTx(txId);
            case 2, 3 -> new ExecutedTx(txId, instantFromMicros(micros), kind == 2, abort);
            default -> throw AdbcException.internal("Unknown native transaction result kind: " + kind);
        };
    }

    static Instant instantFromMicros(long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }
    public String getAwaitToken() throws AdbcException {
        long token = abi.handle("session_await_token", handle());
        if (token == 0) return null;
        try { return new String(abi.copiedBytes(token), java.nio.charset.StandardCharsets.UTF_8); } finally { abi.dispose(token, false); }
    }
    public boolean supportsTypedAwaitToken() { return abi.supports("session_set_await_token"); }
    public boolean supportsTypedReadSettings() { return abi.supports("session_set_read_settings"); }
    public boolean tryBeginReadOnly(String timezone, String snapshotToken, Object snapshotTime, Object clockTime) throws AdbcException {
        if (!abi.supports("session_begin_read_only")) return false;
        changeReadSettings("session_begin_read_only", timezone, snapshotToken, snapshotTime, clockTime, true);
        return true;
    }
    public void setAwaitToken(String token) throws AdbcException {
        try (Operation operation = operation(); Arena arena = Arena.ofConfined()) {
            var text = NativeAbi.optionalText(arena, token);
            abi.call("session_set_await_token", handle(), operation.id(), text, text.byteSize());
        }
    }
    public void setReadSettings(String timezone, String snapshotToken, Object snapshotTime, Object clockTime) throws AdbcException {
        changeReadSettings("session_set_read_settings", timezone, snapshotToken, snapshotTime, clockTime, true);
    }
    private void changeReadSettings(String function, String timezone, String snapshotToken, Object snapshotTime, Object clockTime,
                                    boolean transitPrecision) throws AdbcException {
        try (Operation operation = operation(); Arena arena = Arena.ofConfined()) {
            var zone = NativeAbi.optionalText(arena, timezone);
            var snapshot = NativeAbi.optionalText(arena, snapshotToken);
            abi.call(function, handle(), operation.id(), zone, zone.byteSize(), snapshot, snapshot.byteSize(),
                    readTime(arena, snapshotTime, transitPrecision), readTime(arena, clockTime, transitPrecision));
        }
    }
    private static MemorySegment readTime(Arena arena, Object value, boolean transitPrecision) {
        if (value == null) return MemorySegment.NULL;
        var time = arena.allocate(32, 8);
        if (value instanceof Instant instant) {
            time.set(NativeAbi.I64, 16, instant.getEpochSecond());
            int nanos = instant.getNano();
            time.set(NativeAbi.I32, 24, transitPrecision ? nanos / 1000 * 1000 : nanos);
            time.set(NativeAbi.I32, 28, 2);
        } else {
            int kind = switch (value) {
                case String ignored -> 1;
                case LocalDate ignored -> 3;
                case LocalDateTime ignored -> 4;
                case ZonedDateTime ignored -> 5;
                case OffsetDateTime ignored -> 5;
                default -> throw new IllegalArgumentException("Unsupported read time: " + value.getClass().getName());
            };
            var text = NativeAbi.text(arena, value.toString());
            time.set(NativeAbi.PTR, 0, text);
            time.set(NativeAbi.I64, 8, text.byteSize());
            time.set(NativeAbi.I32, 28, kind);
        }
        return time;
    }
    /** Native transaction request encoded by xtdb.rust; returns the transaction key as Transit JSON. */
    public byte[] transact(byte[] request) throws AdbcException {
        try (Operation operation = operation(); Arena arena = Arena.ofConfined()) {
            MemorySegment input = NativeAbi.bytes(arena, request);
            long result = abi.handle("session_transact", handle(), operation.id(), input, input.byteSize());
            try { return abi.copiedBytes(result); } finally { abi.dispose(result, false); }
        }
    }

    /** Submits transaction operations to this connection's database without waiting for indexing. */
    public SubmittedTx submitTx(List<? extends TxOperation> operations, TxOptions options) throws AdbcException {
        return (SubmittedTx) transactResult(operations, options, true);
    }

    /** Executes transaction operations atomically and returns the indexed outcome, including an abort. */
    public ExecutedTx executeTx(List<? extends TxOperation> operations, TxOptions options) throws AdbcException {
        return (ExecutedTx) transactResult(operations, options, false);
    }

    private CommitOutcome transactResult(List<? extends TxOperation> operations, TxOptions options, boolean asynchronous) throws AdbcException {
        var encodedOperations = operations.stream().map(NativeConnection::encodeOperation).toList();
        var encodedOptions = new LinkedHashMap<String, Object>();
        if (options != null) {
            if (options.defaultTimezone() != null) encodedOptions.put("default_timezone", options.defaultTimezone().getId());
            if (options.systemTime() != null) encodedOptions.put("system_time", options.systemTime());
            if (options.metadata() != null) encodedOptions.put("metadata", options.metadata());
        }
        byte[] request = (byte[]) TransactionCodec.WRITE.invoke(Map.of(
            "operations", encodedOperations, "options", encodedOptions, "asynchronous", asynchronous));
        try (Operation operation = operation(); Arena arena = Arena.ofConfined()) {
            var input = NativeAbi.bytes(arena, request);
            var out = arena.allocate(COMMIT_RESULT);
            abi.call("session_transact_result", handle(), operation.id(), input, input.byteSize(), out);
            return decodeCommitResult(out);
        }
    }

    private static Map<String, Object> encodeOperation(TxOperation operation) {
        return switch (operation) {
            case SqlOperation sql -> sql.argumentRows() == null
                ? Map.of("op", "sql", "sql", sql.sql())
                : Map.of("op", "sql", "sql", sql.sql(), "arguments", sql.argumentRows());
            case PutDocuments put -> documentOperation("put-docs", put.table(), "documents", put.documents(), put.validFrom(), put.validTo());
            case PatchDocuments patch -> documentOperation("patch-docs", patch.table(), "documents", patch.documents(), patch.validFrom(), patch.validTo());
            case DeleteDocuments delete -> documentOperation("delete-docs", delete.table(), "ids", delete.ids(), delete.validFrom(), delete.validTo());
            case EraseDocuments erase -> documentOperation("erase-docs", erase.table(), "ids", erase.ids(), null, null);
        };
    }

    private static Map<String, Object> documentOperation(String kind, TableRef table, String field, List<?> values,
                                                         Instant validFrom, Instant validTo) {
        var operation = new LinkedHashMap<String, Object>();
        operation.put("op", kind);
        operation.put("schema", table.getSchemaName());
        operation.put("table", table.getTableName());
        operation.put(field, values);
        if (validFrom != null) operation.put("valid_from", validFrom);
        if (validTo != null) operation.put("valid_to", validTo);
        return operation;
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
    public NativeRows tryQueryLogicalRelation(String plan, Map<?, ?> arguments) throws AdbcException {
        if (!abi.supports("session_query_ra_arrow")) return null;
        try (var root = NativeArguments.create(allocator, arguments)) {
            if (root == null) return null;
            var operation = operation();
            try (Arena arena = Arena.ofConfined(); ArrowArray array = ArrowArray.allocateNew(allocator);
                 ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
                try {
                    Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
                    var source = NativeAbi.text(arena, plan);
                    long cursor = abi.handle("session_query_ra_arrow", handle(), operation.id(), source, source.byteSize(),
                            NativeAbi.address(array.memoryAddress()), NativeAbi.address(schema.memoryAddress()));
                    return new NativeRows(this, cursor, operation);
                } finally {
                    if (array.snapshot().release != 0) array.release();
                    if (schema.snapshot().release != 0) schema.release();
                }
            } catch (Throwable error) {
                try { operation.close(); } catch (AdbcException close) { error.addSuppressed(close); }
                throw error;
            }
        }
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
