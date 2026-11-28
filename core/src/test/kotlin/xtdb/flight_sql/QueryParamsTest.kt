package xtdb.flight_sql

import kotlinx.serialization.json.Json
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.util.Base64

class QueryParamsTest {
    @Test
    fun `plain Flight parameter fields survive the upload without reading the ticket body`() {
        RootAllocator().use { allocator ->
            val fields = listOf(Field.notNullable("z", ArrowType.Int(64, true)), Field.nullable("a", ArrowType.Utf8()))
            val params = VectorSchemaRoot.create(Schema(fields), allocator).use { root ->
                root.allocateNew()
                (root.getVector(0) as BigIntVector).setSafe(0, 42)
                (root.getVector(1) as VarCharVector).setNull(0)
                root.rowCount = 1

                QueryParams.fromFlightRoot(root).also { params ->
                    val expected = ByteArrayOutputStream()
                    ArrowStreamWriter(root, null, expected).use { writer ->
                        writer.start()
                        writer.writeBatch()
                        writer.end()
                    }
                    val encoded = Json.decodeFromString<String>(Json.encodeToString(QueryParams.Serde, params))
                    assertArrayEquals(expected.toByteArray(), Base64.getDecoder().decode(encoded))
                }
            }

            RootAllocator(0).use { noBatchMemory ->
                assertEquals(fields, params.parameterFields(noBatchMemory))
                val fromTicket = Json.decodeFromString(QueryParams.Serde, Json.encodeToString(QueryParams.Serde, params))
                assertThrows(OutOfMemoryException::class.java) { fromTicket.parameterFields(noBatchMemory) }
                assertEquals(fields, fromTicket.parameterFields(allocator))
            }
        }
    }

    @Test
    fun `parameter field metadata still takes the checked first batch read`() {
        RootAllocator().use { allocator ->
            val field = Field("arg", FieldType(true, ArrowType.Utf8(), null, mapOf("example" to "preserve me")), emptyList())
            val params = VectorSchemaRoot.create(Schema(listOf(field)), allocator).use { root ->
                (root.getVector(0) as VarCharVector).setSafe(0, "value".toByteArray())
                root.rowCount = 1
                QueryParams.fromFlightRoot(root)
            }

            RootAllocator(0).use { noBatchMemory ->
                assertThrows(OutOfMemoryException::class.java) { params.parameterFields(noBatchMemory) }
            }
            assertEquals(listOf(field), params.parameterFields(allocator))
        }
    }
}
