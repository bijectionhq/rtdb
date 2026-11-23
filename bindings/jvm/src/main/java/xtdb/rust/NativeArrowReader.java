package xtdb.rust;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

final class NativeArrowReader extends ArrowReader {
    private final NativeAbi abi;
    private final AtomicLong cursor;
    private final NativeConnection.Operation operation;
    NativeArrowReader(NativeConnection connection, long cursor, NativeConnection.Operation operation) {
        super(connection.allocator); this.abi = connection.abi; this.cursor = new AtomicLong(cursor); this.operation = operation;
    }
    @Override protected Schema readSchema() throws IOException {
        try { return cursorSchema(abi, allocator, cursor.get()); }
        catch (AdbcException error) { throw new IOException(error); }
    }
    static Schema cursorSchema(NativeAbi abi, BufferAllocator allocator, long cursor) throws AdbcException {
        try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
            try { abi.call("cursor_schema", cursor, NativeAbi.address(schema.memoryAddress())); return Data.importSchema(allocator, schema, null, false); }
            finally { if (schema.snapshot().release != 0) schema.release(); }
        }
    }
    @Override public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        try (Arena arena = Arena.ofConfined(); ArrowArray array = ArrowArray.allocateNew(allocator)) {
            try {
                MemorySegment present = arena.allocate(NativeAbi.I32);
                try {
                    if (abi.supports("cursor_next_array")) {
                        abi.call("cursor_next_array", cursor.get(), NativeAbi.address(array.memoryAddress()), present);
                        if (present.get(NativeAbi.I32, 0) == 0) return false;
                        Data.importIntoVectorSchemaRoot(allocator, array, getVectorSchemaRoot(), null, false);
                    } else {
                        try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
                            try {
                                abi.call("cursor_next", cursor.get(), NativeAbi.address(array.memoryAddress()), NativeAbi.address(schema.memoryAddress()), present);
                                if (present.get(NativeAbi.I32, 0) == 0) return false;
                                try (VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, array, schema, null, false)) {
                                    loadRecordBatch(new VectorUnloader(root).getRecordBatch());
                                }
                            } finally { if (schema.snapshot().release != 0) schema.release(); }
                        }
                    }
                } catch (RuntimeException error) {
                    try { abi.call("cursor_consumer_error", cursor.get()); } catch (AdbcException metric) { error.addSuppressed(metric); }
                    throw error;
                }
                return true;
            } finally { if (array.snapshot().release != 0) array.release(); }
        } catch (AdbcException error) { throw new IOException(error); }
    }
    @Override public long bytesRead() { return -1; }
    @Override protected void closeReadSource() throws IOException {
        AdbcException failure = null;
        try { abi.dispose(cursor.getAndSet(0), true); } catch (AdbcException error) { failure = error; }
        try { operation.close(); } catch (AdbcException error) { if (failure == null) failure = error; else failure.addSuppressed(error); }
        if (failure != null) throw new IOException(failure);
    }
}
