package xtdb.rust;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/** Bound Arrow arguments are copied before bind returns; result buffers are independently retained. */
public final class NativeStatement extends NativeResource implements AdbcStatement {
    private final NativeConnection connection;
    private final AtomicReference<NativeConnection.Operation> running = new AtomicReference<>();
    private volatile NativeConnection.Operation latestQuery;
    NativeStatement(NativeConnection connection, long handle) { super(connection.abi, handle); this.connection = connection; }
    @Override public void setSqlQuery(String sql) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) { MemorySegment text = NativeAbi.text(arena, sql); abi.call("statement_set_sql", handle(), text, text.byteSize()); }
    }
    @Override public void prepare() throws AdbcException {
        try (NativeConnection.Operation operation = connection.operation()) {
            running.set(operation);
            try { abi.call("statement_prepare", handle(), operation.id()); } finally { running.compareAndSet(operation, null); }
        }
    }
    @Override public void bind(VectorSchemaRoot root) throws AdbcException {
        if (abi.supports("statement_bind_retained")) {
            try (VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), connection.allocator);
                 ArrowRecordBatch source = new VectorUnloader(root).getRecordBatch()) {
                var buffers = new ArrayList<ArrowBuf>(source.getBuffers().size());
                try {
                    for (ArrowBuf input : source.getBuffers()) {
                        long size = input.readableBytes();
                        ArrowBuf buffer = connection.allocator.buffer(size);
                        buffers.add(buffer);
                        buffer.setBytes(0, input, input.readerIndex(), size);
                        buffer.writerIndex(size);
                    }
                    try (var batch = new ArrowRecordBatch(source.getLength(), source.getNodes(), buffers,
                            source.getBodyCompression(), source.getVariadicBufferCounts(), true)) {
                        new VectorLoader(copy).load(batch);
                    }
                    bindFromAllocator(copy, connection.allocator, "statement_bind_retained");
                } finally { buffers.forEach(ArrowBuf::close); }
            }
            return;
        }
        BufferAllocator allocator = root.getFieldVectors().isEmpty() ? connection.allocator : root.getVector(0).getAllocator();
        if (root.getFieldVectors().stream().allMatch(vector -> vector.getAllocator().getRoot() == allocator.getRoot())) {
            bindFromAllocator(root, allocator, "statement_bind");
            return;
        }
        try (VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), connection.allocator)) {
            copy.allocateNew();
            for (int column = 0; column < root.getFieldVectors().size(); column++) {
                var source = root.getVector(column);
                var target = copy.getVector(column);
                for (int row = 0; row < root.getRowCount(); row++) target.copyFromSafe(row, row, source);
                target.setValueCount(root.getRowCount());
            }
            copy.setRowCount(root.getRowCount());
            bindFromAllocator(copy, connection.allocator, "statement_bind");
        }
    }
    public boolean tryBindNativeArguments(List<?> arguments) throws AdbcException {
        try (VectorSchemaRoot root = NativeArguments.positional(connection.allocator, arguments)) {
            if (root == null) return false;
            String entryPoint = abi.supports("statement_bind_retained") ? "statement_bind_retained" : "statement_bind";
            bindFromAllocator(root, connection.allocator, entryPoint);
            return true;
        } catch (OutOfMemoryException exhausted) {
            return false;
        } catch (NativeException error) {
            if ("rtdb/memory-budget-exceeded".equals(error.code())) return false;
            throw error;
        }
    }
    private void bindFromAllocator(VectorSchemaRoot root, BufferAllocator allocator, String entryPoint) throws AdbcException {
        try (ArrowArray array = ArrowArray.allocateNew(allocator); ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
            try {
                Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
                abi.call(entryPoint, handle(), NativeAbi.address(array.memoryAddress()), NativeAbi.address(schema.memoryAddress()));
            } finally {
                if (array.snapshot().release != 0) array.release();
                if (schema.snapshot().release != 0) schema.release();
            }
        }
    }
    /** Accepts a Transit MessagePack array of positional argument rows; an empty array is an explicit empty batch. */
    public void bindTransit(byte[] rows) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) { MemorySegment bytes = NativeAbi.bytes(arena, rows); abi.call("statement_bind_transit", handle(), bytes, bytes.byteSize()); }
    }
    @Override public Schema getParameterSchema() throws AdbcException { return schema(true); }
    @Override public Schema executeSchema() throws AdbcException { return schema(false); }
    private Schema schema(boolean parameters) throws AdbcException {
        try (NativeConnection.Operation operation = connection.operation(); ArrowSchema schema = ArrowSchema.allocateNew(connection.allocator)) {
            running.set(operation);
            try {
                abi.call("statement_schema", handle(), operation.id(), parameters ? 1 : 0, NativeAbi.address(schema.memoryAddress()));
                return Data.importSchema(connection.allocator, schema, null, false);
            } finally { running.compareAndSet(operation, null); if (schema.snapshot().release != 0) schema.release(); }
        }
    }
    private record Opened(long cursor, NativeConnection.Operation operation) {}
    private Opened open() throws AdbcException {
        NativeConnection.Operation operation = connection.operation();
        running.set(operation);
        try {
            long cursor = abi.handle("statement_query", handle(), operation.id());
            latestQuery = operation;
            return new Opened(cursor, operation);
        } catch (Throwable error) { try { operation.close(); } catch (AdbcException close) { error.addSuppressed(close); } throw error; }
        finally { running.compareAndSet(operation, null); }
    }
    @Override public QueryResult executeQuery() throws AdbcException {
        Opened opened = open();
        return new QueryResult(-1, new NativeArrowReader(connection, opened.cursor(), opened.operation()));
    }
    public NativeRows executeRows() throws AdbcException { Opened opened = open(); return new NativeRows(connection, opened.cursor(), opened.operation()); }
    @Override public UpdateResult executeUpdate() throws AdbcException {
        try (NativeConnection.Operation operation = connection.operation(); Arena arena = Arena.ofConfined()) {
            running.set(operation);
            try { MemorySegment out = arena.allocate(NativeAbi.I64); abi.call("statement_update", handle(), operation.id(), out); return new UpdateResult(out.get(NativeAbi.I64, 0)); }
            finally { running.compareAndSet(operation, null); }
        }
    }
    @Override public void cancel() throws AdbcException {
        NativeConnection.Operation operation = running.get();
        if (operation == null) operation = latestQuery;
        if (operation != null) operation.cancel();
    }
    @Override public void close() throws AdbcException {
        NativeConnection.Operation operation = running.get();
        if (operation != null) operation.cancel();
        super.close();
    }
}
