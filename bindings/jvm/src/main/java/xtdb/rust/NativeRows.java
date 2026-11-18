package xtdb.rust;

import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/** Bounded Transit JSON batches; rtdb/struct tags distinguish document field names from map keys. */
public final class NativeRows extends NativeResource {
    private final NativeConnection.Operation operation;
    private final BufferAllocator allocator;
    NativeRows(NativeConnection connection, long cursor, NativeConnection.Operation operation) {
        super(connection.abi, cursor);
        this.operation = operation;
        this.allocator = connection.allocator;
    }
    public Schema schema() throws AdbcException { return NativeArrowReader.cursorSchema(abi, allocator, handle()); }
    /** Returns null at end-of-stream. Each non-null byte array owns one batch independently. */
    public byte[] next() throws AdbcException {
        long bytes = abi.handle("cursor_next_transit", handle());
        if (bytes == 0) return null;
        try { return abi.copiedBytes(bytes); } finally { abi.dispose(bytes, false); }
    }
    public void cancel() throws AdbcException { operation.cancel(); }
    @Override public void close() throws AdbcException { try { super.close(); } finally { operation.close(); } }
}
