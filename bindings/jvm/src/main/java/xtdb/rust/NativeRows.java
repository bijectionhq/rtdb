package xtdb.rust;

import org.apache.arrow.adbc.core.AdbcException;

/** Bounded Transit JSON batches; rtdb/struct tags distinguish document field names from map keys. */
public final class NativeRows extends NativeResource {
    private final NativeConnection.Operation operation;
    NativeRows(NativeAbi abi, long cursor, NativeConnection.Operation operation) { super(abi, cursor); this.operation = operation; }
    /** Returns null at end-of-stream. Each non-null byte array owns one batch independently. */
    public byte[] next() throws AdbcException {
        long bytes = abi.handle("cursor_next_transit", handle());
        if (bytes == 0) return null;
        try { return abi.copiedBytes(bytes); } finally { abi.dispose(bytes, false); }
    }
    public void cancel() throws AdbcException { operation.cancel(); }
    @Override public void close() throws AdbcException { try { super.close(); } finally { operation.close(); } }
}
