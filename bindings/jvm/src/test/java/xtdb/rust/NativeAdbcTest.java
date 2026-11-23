package xtdb.rust;

import static org.junit.jupiter.api.Assertions.*;
import clojure.java.api.Clojure;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import xtdb.api.error.Incorrect;

final class NativeAdbcTest {
    private static final String YAML = "server: null\nflightSql: null\nnodeId: jvm-binding-test\n";
    private static NativeDatabase open(RootAllocator allocator) throws Exception { return NativeDatabase.open(allocator, YAML, 512L * 1024 * 1024); }

    @Test void setsTheEmbeddedApplicationName() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect()) {
            assertEquals("XTDB", applicationName(connection));
            connection.setApplicationName("météo's importer");
            assertEquals("météo's importer", applicationName(connection));
        }
    }

    private static String applicationName(NativeConnection connection) throws Exception {
        try (var show = connection.createStatement("SHOW application_name"); var result = show.executeQuery()) {
            assertTrue(result.getReader().loadNextBatch());
            return result.getReader().getVectorSchemaRoot().getVector(0).getObject(0).toString();
        }
    }

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

    @Test void logClockRejectsFailedReadsAndAllowsTheNextAppend() throws Exception {
        var reads = new AtomicInteger();
        try (var allocator = new RootAllocator();
             var database = NativeDatabase.open(allocator, YAML, 512L * 1024 * 1024, () -> {
                 return switch (reads.getAndIncrement()) {
                     case 0 -> throw new IllegalStateException("clock unavailable");
                     case 1 -> Instant.MAX;
                     default -> Instant.parse("2020-01-02T03:04:05.123456789Z");
                 };
             });
             var connection = database.connect();
             var insert = connection.createStatement("INSERT INTO docs (_id) VALUES (1)")) {
            assertEquals(0, reads.get());
            var failed = assertThrows(NativeException.class, insert::executeUpdate);
            assertTrue(failed.getMessage().contains("The host log clock failed"));
            var outOfRange = assertThrows(NativeException.class, insert::executeUpdate);
            assertTrue(outOfRange.getMessage().contains("Instant exceeds microsecond timestamp range"));
            insert.executeUpdate();
            try (var statement = connection.createStatement("SELECT CAST(_valid_from AS TEXT) FROM docs");
                 var result = statement.executeQuery()) {
                assertTrue(result.getReader().loadNextBatch());
                var batch = result.getReader().getVectorSchemaRoot();
                assertEquals(1, batch.getRowCount());
                var validFrom = ZonedDateTime.parse(batch.getVector(0).getObject(0).toString()).toInstant();
                assertEquals(Instant.parse("2020-01-02T03:04:05.123456Z"), validFrom);
                assertFalse(result.getReader().loadNextBatch());
            }
            assertEquals(3, reads.get());
        }
        assertEquals(3, reads.get());
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

    @Test void embeddedSqlErrorsExposeDiagnosticsAndKeepTheConnectionUsable() throws Exception {
        Clojure.var("clojure.core", "require").invoke(Clojure.read("xtdb.rust"));
        var query = Clojure.var("xtdb.api", "q");
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect()) {
            for (var expected : Map.of(
                    "SELEC 1", "Errors parsing SQL statement:\n  - line 1:0 mismatched input 'SELEC' expecting ",
                    "extra SELECT 1", "Errors parsing SQL statement:\n  - line 1:0 extraneous input 'extra' expecting ").entrySet()) {
                String sql = expected.getKey();
                NativeException diagnostic;
                try (var statement = connection.createStatement(sql)) {
                    diagnostic = assertThrows(NativeException.class, statement::prepare);
                }
                assertEquals(AdbcStatusCode.INVALID_ARGUMENT, diagnostic.getStatus());
                assertEquals("incorrect", diagnostic.category());
                assertEquals("xtdb/sql-error", diagnostic.code());
                assertTrue(diagnostic.getMessage().startsWith(expected.getValue()), diagnostic.getMessage());
                assertTrue(diagnostic.transit().length > 0);
                assertNull(diagnostic.getCause());

                var anomaly = assertThrows(Incorrect.class, () -> query.invoke(connection, sql));
                assertEquals(diagnostic.getMessage(), anomaly.getMessage());
                assertEquals(Clojure.read(":cognitect.anomalies/incorrect"), anomaly.getData().valAt(Clojure.read(":cognitect.anomalies/category")));
                assertEquals(Clojure.read(":xtdb/sql-error"), anomaly.getData().valAt(Clojure.read(":xtdb.error/code")));
                assertEquals(sql, anomaly.getData().valAt(Clojure.read(":sql")));
                assertInstanceOf(Incorrect.class, anomaly.getCause());
                assertNull(anomaly.getCause().getCause());
            }
            try (var statement = connection.createStatement("SELECT 42 AS answer"); var result = statement.executeQuery()) {
                assertTrue(result.getReader().loadNextBatch());
                assertEquals(42, ((Number) result.getReader().getVectorSchemaRoot().getVector("answer").getObject(0)).intValue());
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
                try (var root = new VectorSchemaRoot(List.of(first, second))) {
                    root.setRowCount(1);
                    statement.bind(root);
                    first.set(0, 300);
                    second.set(0, 700);
                }
            }
            try (var result = statement.executeQuery()) {
                assertTrue(result.getReader().loadNextBatch());
                assertEquals(3L, result.getReader().getVectorSchemaRoot().getVector("a").getObject(0));
                assertEquals(7L, result.getReader().getVectorSchemaRoot().getVector("b").getObject(0));
            }
        }
    }

    @Test void logicalRelationBindsNamedArrowColumnsAndScopesTheRowStream() throws Exception {
        try (var allocator = new RootAllocator(); var database = open(allocator); var connection = database.connect()) {
            String plan = "[:project {:projections [{v ?parameter}]} [:table {:rows [{}]}]]";
            try (var rows = connection.tryQueryLogicalRelation(plan, Map.of("parameter", List.of(-0.0f, 0.25f)))) {
                assertNotNull(rows);
                var input = rows.readNext(stream -> {
                    try {
                        assertEquals("[[\"~#rtdb/struct\",[\"^ \",\"v\",[[\"~#f32\",-0.0],[\"~#f32\",0.25]]]]]",
                                new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
                        return stream;
                    } catch (IOException error) {
                        throw new java.io.UncheckedIOException(error);
                    }
                });
                assertNotNull(input);
                assertNull(rows.readNext(stream -> fail("extra result batch")));
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
