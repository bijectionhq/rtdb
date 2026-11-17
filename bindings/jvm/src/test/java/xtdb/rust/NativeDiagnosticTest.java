package xtdb.rust;

import static org.junit.jupiter.api.Assertions.*;
import clojure.java.api.Clojure;
import clojure.lang.PersistentArrayMap;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.junit.jupiter.api.Test;

final class NativeDiagnosticTest {
    @Test void nativeMessageSlicesPreserveAbsentEmptyAndUtf8Messages() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment slice = arena.allocate(16, 8);
            slice.set(NativeAbi.PTR, 0, MemorySegment.NULL);
            slice.set(NativeAbi.I64, 8, 0);
            assertNull(NativeAbi.diagnosticMessage(slice));
            slice.set(NativeAbi.PTR, 0, arena.allocate(1));
            assertEquals("", NativeAbi.diagnosticMessage(slice));
            MemorySegment message = NativeAbi.text(arena, "échec");
            slice.set(NativeAbi.PTR, 0, message);
            slice.set(NativeAbi.I64, 8, message.byteSize());
            assertEquals("échec", NativeAbi.diagnosticMessage(slice));
            slice.set(NativeAbi.PTR, 0, MemorySegment.NULL);
            assertThrows(AdbcException.class, () -> NativeAbi.diagnosticMessage(slice));
        }
    }

    @Test void adbcExceptionsRetainNullAndEmptyMessagesWithTheirDiagnostics() {
        byte[] wire = {1, 2, 3};
        NativeException absent = new NativeException("fault", "test/error", null, wire);
        NativeException empty = new NativeException("fault", "test/error", "", wire);
        assertNull(absent.getMessage());
        assertEquals("", empty.getMessage());
        assertEquals(AdbcStatusCode.INTERNAL, absent.getStatus());
        assertEquals("fault", absent.category());
        assertEquals("test/error", absent.code());
        wire[0] = 4;
        assertArrayEquals(new byte[] {1, 2, 3}, absent.transit());
        byte[] copy = absent.transit(); copy[0] = 5;
        assertArrayEquals(new byte[] {1, 2, 3}, absent.transit());
    }

    @Test void clojureBridgePreservesNullableTransitMessages() {
        Clojure.var("clojure.core", "require").invoke(Clojure.read("xtdb.rust"));
        for (String message : new String[] {null, "", "detail"}) {
            Object anomaly = Clojure.var("xtdb.error", "fault").invoke(Clojure.read(":test/error"), message);
            byte[] wire = (byte[]) Clojure.var("xtdb.serde", "write-transit").invoke(anomaly);
            NativeException error = new NativeException("fault", "test/error", message, wire);
            Throwable decoded = (Throwable) Clojure.var("xtdb.error", "->anomaly").invoke(error, PersistentArrayMap.EMPTY);
            assertEquals(message, decoded.getMessage());
            assertEquals("xtdb.api.error.Fault", decoded.getClass().getName());
        }
    }
}
