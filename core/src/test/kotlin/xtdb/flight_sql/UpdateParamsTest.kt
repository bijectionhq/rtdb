package xtdb.flight_sql

import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class UpdateParamsTest {
    @Test
    fun `a plain update owns its batch and each read gets independent Arrow buffers`() {
        val fields = listOf(Field.nullable("", ArrowType.Int(64, true)), Field.nullable("", ArrowType.Utf8()))
        val (params, ipcBytes) = RootAllocator().use { uploadAllocator ->
            VectorSchemaRoot.create(Schema(fields), uploadAllocator).use { root ->
                root.allocateNew()
                val ints = root.getVector(0) as BigIntVector
                val strs = root.getVector(1) as VarCharVector
                ints.setSafe(0, Long.MIN_VALUE)
                ints.setNull(1)
                strs.setNull(0)
                strs.setSafe(1, "café".toByteArray())
                root.rowCount = 2

                val ipcBytes = RootAllocator().use { ipcAllocator ->
                    QueryParams.fromFlightRoot(root).read(ipcAllocator) { ipcAllocator.allocatedMemory }
                }
                checkNotNull(UpdateParams.fromFlightRoot(root)) to ipcBytes
            }
        }

        RootAllocator(0).use { noBatchMemory ->
            assertThrows<OutOfMemoryException> { params.read(noBatchMemory) {} }
        }
        RootAllocator().use { allocator ->
            params.read(allocator) { first ->
                assertEquals(ipcBytes, allocator.allocatedMemory)
                assertEquals(fields, first.schema.fields)
                val ints = first.getVector(0) as BigIntVector
                ints.set(0, 17)
                params.read(allocator) { second ->
                    val secondInts = second.getVector(0) as BigIntVector
                    val strs = second.getVector(1) as VarCharVector
                    assertEquals(2, second.rowCount)
                    assertEquals(Long.MIN_VALUE, secondInts.get(0))
                    assertNull(secondInts.getObject(1))
                    assertNull(strs.getObject(0))
                    assertEquals("café", strs.getObject(1).toString())
                }
                assertEquals(17L, ints.get(0))
            }
            assertEquals(0L, allocator.allocatedMemory)
        }
    }

    @Test
    fun `a zero row update keeps its parameter columns`() {
        val fields = listOf(Field.nullable("arg", ArrowType.Int(64, true)))
        RootAllocator().use { allocator ->
            val params = VectorSchemaRoot.create(Schema(fields), allocator).use { root ->
                checkNotNull(UpdateParams.fromFlightRoot(root))
            }
            params.read(allocator) { root ->
                assertEquals(0, root.rowCount)
                assertEquals(fields, root.schema.fields)
            }
            assertEquals(0L, allocator.allocatedMemory)
        }
    }

    @Test
    fun `an annotated update keeps the checked IPC layout`() {
        val field = Field("arg", FieldType(true, ArrowType.Utf8(), null, mapOf("example" to "keep")), emptyList())
        RootAllocator().use { allocator ->
            VectorSchemaRoot.create(Schema(listOf(field)), allocator).use { root ->
                assertNull(UpdateParams.fromFlightRoot(root))
            }
        }
    }
}
