package xtdb.rust;

import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.function.Function;
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
        return nextBytes(bytes -> bytes.toArray(ValueLayout.JAVA_BYTE));
    }
    /** Reads one batch from its native byte lease. The stream is valid only during the callback. */
    public <T> T readNext(Function<InputStream, T> read) throws AdbcException {
        return nextBytes(bytes -> read.apply(new InputStream() {
            private long position;
            @Override public int read() {
                return position == bytes.byteSize() ? -1 : Byte.toUnsignedInt(bytes.get(ValueLayout.JAVA_BYTE, position++));
            }
            @Override public int read(byte[] target, int offset, int length) {
                Objects.checkFromIndexSize(offset, length, target.length);
                if (length == 0) return 0;
                int count = (int) Math.min(length, bytes.byteSize() - position);
                if (count == 0) return -1;
                MemorySegment.copy(bytes, position, MemorySegment.ofArray(target), offset, count);
                position += count;
                return count;
            }
        }));
    }
    private <T> T nextBytes(Function<MemorySegment, T> read) throws AdbcException {
        try (Arena arena = Arena.ofConfined()) {
            var view = arena.allocate(16, 8);
            long bytes = abi.handle("cursor_next_row_bytes", handle(), view);
            if (bytes == 0) return null;
            try (Arena lease = Arena.ofConfined()) {
                var segment = view.get(NativeAbi.PTR, 0).reinterpret(view.get(NativeAbi.I64, 8), lease, null);
                return read.apply(segment);
            } finally { abi.dispose(bytes, false); }
        }
    }
    public void cancel() throws AdbcException { operation.cancel(); }
    @Override public void close() throws AdbcException { try { super.close(); } finally { operation.close(); } }
}
