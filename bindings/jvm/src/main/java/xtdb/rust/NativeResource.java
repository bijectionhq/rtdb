package xtdb.rust;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.adbc.core.AdbcException;

abstract class NativeResource implements AutoCloseable {
    final NativeAbi abi;
    private final AtomicLong handle;
    NativeResource(NativeAbi abi, long handle) { this(abi, new AtomicLong(handle)); }
    NativeResource(NativeAbi abi, AtomicLong ownership) { this.abi = abi; this.handle = ownership; }
    final long handle() throws AdbcException {
        long value = handle.get();
        if (value == 0) throw AdbcException.invalidState("Native resource is closed");
        return value;
    }
    final long retainedHandle() throws AdbcException { return abi.handle("retain", handle()); }
    final void dispose(boolean close) throws AdbcException { abi.dispose(handle.getAndSet(0), close); }
    @Override public void close() throws AdbcException { dispose(true); }
}
