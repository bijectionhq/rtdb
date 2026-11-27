package xtdb.tx

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import xtdb.arrow.DateDayVector
import xtdb.arrow.DateMilliVector
import xtdb.arrow.StructVector
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
}
