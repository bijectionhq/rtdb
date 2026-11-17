package xtdb.rust;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

final class NativeAdbcTest {
    private static final String YAML = "server: null\nflightSql: null\nnodeId: jvm-binding-test\n";
    private static NativeDatabase open(RootAllocator allocator) throws Exception { return NativeDatabase.open(allocator, YAML, 512L * 1024 * 1024); }

    @Test void explicitObjectSorterSelectionOpensIndependentNativeRuntimes() throws Exception {
        for (boolean legacy : new boolean[] {false, true}) {
            try (var allocator = new RootAllocator(); var database = NativeDatabase.open(allocator, YAML, 512L * 1024 * 1024, legacy); var connection = database.connect(); var statement = connection.createStatement("SELECT n FROM (VALUES (3), (1), (2), (1)) AS t(n) ORDER BY n")) {
                try (var result = statement.executeQuery()) {
                    assertTrue(result.getReader().loadNextBatch());
                    var column = result.getReader().getVectorSchemaRoot().getVector(0);
                    assertEquals(List.of(1, 1, 2, 3), java.util.stream.IntStream.range(0, 4).mapToObj(i -> ((Number) column.getObject(i)).intValue()).toList());
                    assertFalse(result.getReader().loadNextBatch());
                }
            }
        }
    }

    @Test void adbcTestBasicQueryAndUpdate() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect(); var statement = connection.createStatement()) {
            statement.setSqlQuery("INSERT INTO foo (_id, v) VALUES (1, 'native')");
            assertEquals(-1, statement.executeUpdate().getAffectedRows());
            statement.setSqlQuery("SELECT v FROM foo");
            try (var result = statement.executeQuery()) {
                assertTrue(result.getReader().loadNextBatch());
                assertEquals("native", result.getReader().getVectorSchemaRoot().getVector("v").getObject(0).toString());
                assertFalse(result.getReader().loadNextBatch());
            }
        }
    }

    @Test void adbcTestUnnamedDuplicateParameterColumnsAndExternalAllocatorCopy() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect(); var statement = connection.createStatement("SELECT ? AS a, ? AS b")) {
            statement.prepare();
            assertEquals(List.of("", ""), statement.getParameterSchema().getFields().stream().map(Field::getName).toList());
            try (var external = new RootAllocator(); var other = new RootAllocator(); var first = new BigIntVector("", external); var second = new BigIntVector("", other)) {
                first.allocateNew(1); first.setSafe(0, 3); first.setValueCount(1);
                second.allocateNew(1); second.setSafe(0, 7); second.setValueCount(1);
                try (var root = new VectorSchemaRoot(List.of(first, second))) { root.setRowCount(1); statement.bind(root); }
            }
            try (var result = statement.executeQuery()) {
                assertTrue(result.getReader().loadNextBatch());
                assertEquals(3L, result.getReader().getVectorSchemaRoot().getVector("a").getObject(0));
                assertEquals(7L, result.getReader().getVectorSchemaRoot().getVector("b").getObject(0));
            }
        }
    }

    @Test void retainedArrowBatchOutlivesStatementCursorAndDatabase() throws Exception {
        try (var allocator = new RootAllocator()) {
            ArrowRecordBatch retained;
            try (var database = open(allocator); var connection = database.connect(); var statement = connection.createStatement("SELECT 42 AS answer")) {
                try (var result = statement.executeQuery()) {
                    assertTrue(result.getReader().loadNextBatch());
                    retained = new VectorUnloader(result.getReader().getVectorSchemaRoot()).getRecordBatch();
                    statement.close();
                }
                assertTrue(database.nativeMemoryUsed() > 0);
                database.close();
            }
            assertEquals(1, retained.getLength());
            assertTrue(retained.getBuffers().stream().anyMatch(buffer -> buffer.readableBytes() > 0));
            retained.close();
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test void cancellingSecondStreamLeavesFirstUsable() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect(); var first = connection.createStatement("SELECT 1 AS v"); var second = connection.createStatement("SELECT 2 AS v")) {
            try (var firstResult = first.executeQuery(); var secondResult = second.executeQuery()) {
                second.cancel();
                assertThrows(IOException.class, () -> secondResult.getReader().loadNextBatch());
                assertTrue(firstResult.getReader().loadNextBatch());
                assertEquals(1, ((Number) firstResult.getReader().getVectorSchemaRoot().getVector(0).getObject(0)).intValue());
            }
        }
    }

    @Test void adbcTestBulkIngestCreateAppendAndTableSchema() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect(); var statement = connection.bulkIngest("custom.docs", BulkIngestMode.CREATE_APPEND); var id = new BigIntVector("_id", allocator)) {
            id.allocateNew(2); id.setSafe(0, 1); id.setSafe(1, 2); id.setValueCount(2);
            try (var root = new VectorSchemaRoot(List.of(id))) { root.setRowCount(2); statement.bind(root); }
            assertEquals(2, statement.executeUpdate().getAffectedRows());
            assertTrue(connection.getTableSchema(null, "custom", "docs").getFields().stream().anyMatch(field -> field.getName().equals("_id")));
            assertEquals(0, statement.executeUpdate().getAffectedRows());
        }
    }

    @Test void adbcMetadataUsesStandardSchemasAndLikeFilters() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect()) {
            try (var info = connection.getInfo(); var tables = connection.getTableTypes(); var objects = connection.getObjects(GetObjectsDepth.ALL, "xt%", "public", "%", null, null)) {
                assertEquals(StandardSchemas.GET_INFO_SCHEMA, info.getVectorSchemaRoot().getSchema());
                assertEquals(StandardSchemas.TABLE_TYPES_SCHEMA, tables.getVectorSchemaRoot().getSchema());
                assertEquals(StandardSchemas.GET_OBJECTS_SCHEMA, objects.getVectorSchemaRoot().getSchema());
                assertTrue(info.loadNextBatch()); assertEquals(4, info.getVectorSchemaRoot().getRowCount());
                assertTrue(objects.loadNextBatch()); assertEquals(1, objects.getVectorSchemaRoot().getRowCount());
            }
        }
    }

    @Test void adbcManualTransactionAndSqlTransactionControlRejection() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect()) {
            assertThrows(NativeException.class, () -> connection.createStatement("BEGIN"));
            connection.setAutoCommit(false);
            try (var insert = connection.createStatement("INSERT INTO docs (_id) VALUES (7)")) { insert.executeUpdate(); }
            try (var query = connection.createStatement("SELECT * FROM docs")) { assertThrows(NativeException.class, query::executeQuery); }
            connection.rollback();
            connection.setAutoCommit(true);
            assertThrows(NativeException.class, connection::commit);
        }
    }
}
