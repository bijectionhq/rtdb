package xtdb.tx

import clojure.lang.Keyword
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import xtdb.arrow.DateDayVector
import xtdb.arrow.DateMilliVector
import xtdb.arrow.LongVector
import xtdb.arrow.Relation
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.StructVector
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.schema
import java.nio.ByteBuffer
import java.time.LocalDate

class TxWriterTest {

    @Test
    fun `source receipt copies present date children and omits null-only fields`() {
        RootAllocator().use { allocator ->
            val date = LocalDate.of(1969, 12, 31)
            val bytes = serializeUserMetadata(allocator, linkedMapOf("absent" to null, "date" to date, "n" to 1L))
            withUserMetadata(allocator, bytes) { source ->
                val write = sourceUserMetadataWriter(source)
                StructVector(allocator, "user_metadata", true).use { dest ->
                    write(dest)
                    assertEquals(listOf("date", "n"), dest.keyNames.toList())
                    val dates = dest.vectorFor("date") as DateDayVector
                    assertEquals(-1, dates.getInt(0))
                    assertFalse(dates.nullable)
                    assertEquals(mapOf("date" to date, "n" to 1L), dest.getObject(0))
                }
                StructVector(allocator, "user_metadata", true).use { dest ->
                    dest.writeObject(mapOf("absent" to "earlier"))
                    write(dest)
                    assertNull(dest.vectorFor("absent").getObject(1))
                    assertEquals(mapOf("date" to date, "n" to 1L), dest.getObject(1))
                }
            }
        }
    }

    @Test
    fun `source receipt keeps object promotion when the destination date field uses another type`() {
        RootAllocator().use { allocator ->
            val date = LocalDate.of(2026, 1, 5)
            val bytes = serializeUserMetadata(allocator, mapOf("date" to date, "absent" to null))
            withUserMetadata(allocator, bytes) { source ->
                StructVector(allocator, "user_metadata", true).use { dest ->
                    dest.writeObject(mapOf("date" to "previous"))
                    sourceUserMetadataWriter(source)(dest)
                    assertEquals(mapOf("date" to "previous"), dest.getObject(0))
                    assertEquals(mapOf("date" to date), dest.getObject(1))
                    assertNull(dest.vectorForOrNull("absent"))
                }
            }
        }
    }

    @Test
    fun `source receipt keeps a predeclared millisecond date and writes compatible siblings once`() {
        RootAllocator().use { allocator ->
            val date = LocalDate.of(1969, 12, 31)
            val bytes = serializeUserMetadata(allocator, linkedMapOf("n" to 2L, "date" to date))
            withUserMetadata(allocator, bytes) { source ->
                StructVector(allocator, "user_metadata", true).use { dest ->
                    val dates = dest.vectorFor("date", ArrowType.Date(DateUnit.MILLISECOND), false) as DateMilliVector
                    dest.writeObject(mapOf("date" to LocalDate.EPOCH, "n" to 1L))
                    sourceUserMetadataWriter(source)(dest)
                    assertSame(dates, dest.vectorFor("date"))
                    assertEquals(-86_400_000L, dates.getLong(1))
                    assertEquals(listOf(1L, 2L), dest.vectorFor("n").asList)
                }
            }
        }
    }

    @Test
    fun `source receipt writes a single run as a plain scalar and preserves Java keyword decoding`() {
        RootAllocator().use { allocator ->
            val metadataType = VectorType.structOf("n" to VectorType.I64.runEndEncoded(), "key" to VectorType.KEYWORD)
            val spellings = listOf("ns/café".toByteArray(), byteArrayOf(0xed.toByte(), 0xa0.toByte(), 0x80.toByte()))
            for (spelling in spellings) {
                val bytes = Relation(allocator, schema("user-metadata" ofType metadataType)).use { rel ->
                    val metadata = rel["user-metadata"] as StructVector
                    (metadata.vectorFor("n") as RunEndEncodedVector).writeRun(42L)
                    metadata.vectorFor("key").writeBytes(spelling)
                    metadata.endStruct()
                    rel.endRow()
                    rel.toArrowStream()
                }

                withUserMetadata(allocator, bytes) { source ->
                    StructVector(allocator, "user_metadata", true).use { dest ->
                        sourceUserMetadataWriter(source)(dest)
                        val numbers = assertInstanceOf(LongVector::class.java, dest.vectorFor("n"))
                        assertFalse(numbers.nullable)
                        assertEquals(42L, numbers.getLong(0))
                        val decoded = spelling.toString(Charsets.UTF_8)
                        assertEquals(mapOf("n" to 42L, "key" to Keyword.intern(decoded)), dest.getObject(0))
                        assertEquals(ByteBuffer.wrap(decoded.toByteArray()), dest.vectorFor("key").getBytes(0))
                    }
                }
            }
        }
    }
}
