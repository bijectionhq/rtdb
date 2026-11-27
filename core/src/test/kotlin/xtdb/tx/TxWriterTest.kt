package xtdb.tx

import clojure.lang.Keyword
import com.google.protobuf.ByteString
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.TaggedValue
import xtdb.asBytes
import xtdb.api.error.Incorrect
import xtdb.arrow.DateDayVector
import xtdb.arrow.DateMilliVector
import xtdb.arrow.DenseUnionVector
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.LongVector
import xtdb.arrow.Relation
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.StructVector
import xtdb.arrow.UuidVector
import xtdb.arrow.Vector
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.extensions.UuidType
import xtdb.arrow.schema
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID

class TxWriterTest {

    @Suppress("DEPRECATION")
    @Test
    fun `delete and erase source records keep ordered canonical IIDs and delete bounds`() {
        RootAllocator().use { allocator ->
            val uuid = UUID.fromString("f1234567-89ab-cdef-fedc-ba9876543210")
            val keyword = Keyword.intern("ns/café")
            val docIds = listOf((-3).toByte(), (-500).toShort(), -10, -10000000000L, uuid, "café", keyword, uuid)
            val validFrom = Instant.parse("1969-12-31T23:59:00Z")
            val validTo = Instant.parse("1970-01-02T00:00:00Z")
            Vector.fromList(allocator, "ids", docIds).use { ids ->
                assertInstanceOf(DenseUnionVector::class.java, ids)
                val ops = listOf(
                    TxOp.DeleteDocs("public", "docs", validFrom, validTo, ids),
                    TxOp.EraseDocs("public", "docs", ids)
                )
                for (legacy in listOf(false, true)) {
                    val bytes = if (legacy) ByteString.copyFrom(ops.toLegacyBytes(allocator, TxOpts(defaultTz = ZoneOffset.UTC)))
                    else ops.toArrowBytes(allocator)
                    Relation.StreamLoader(allocator, Channels.newChannel(bytes.newInput())).use { loader ->
                        Relation(allocator, loader.schema).use { rel ->
                            assertTrue(loader.loadNextPage(rel))
                            val txOps = if (legacy) rel["tx-ops"].listElements else rel["tx-ops"]
                            assertEquals(2, txOps.valueCount)
                            for ((row, name) in listOf("delete-docs", "erase-docs").withIndex()) {
                                assertEquals(name, txOps.getLeg(row))
                                val op = txOps.vectorFor(name)
                                assertEquals("public/docs", op.vectorFor("table").getObject(row))
                                val iids = op.vectorFor("iids")
                                assertEquals(docIds.size, iids.getListCount(row))
                                val start = iids.getListStartIndex(row)
                                for ((idx, id) in docIds.withIndex()) {
                                    assertArrayEquals(id.asIid, iids.listElements.getObject(start + idx) as ByteArray)
                                }
                            }
                            val delete = txOps.vectorFor("delete-docs")
                            assertEquals(validFrom.asMicros, delete.vectorFor("_valid_from").getLong(0))
                            assertEquals(validTo.asMicros, delete.vectorFor("_valid_to").getLong(0))
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `delete and erase refuse the first invalid logical ID with its original diagnostic`() {
        RootAllocator().use { allocator ->
            val uuid = UUID.fromString("f1234567-89ab-cdef-fedc-ba9876543210")
            fun assertRefused(ids: Vector, invalidId: Any?) {
                val expected = assertThrows<Incorrect> { invalidId.asIid }
                for (op in listOf(TxOp.DeleteDocs("public", "docs", ids = ids), TxOp.EraseDocs("public", "docs", ids))) {
                    val actual = assertThrows<Incorrect> { listOf(op).toArrowBytes(allocator) }
                    assertEquals(expected.message, actual.message)
                    assertEquals(expected.getData(), actual.getData())
                }
            }

            DenseUnionVector(allocator, "ids").use { ids ->
                ids.writeObject(uuid)
                ids.writeNull()
                ids.writeUndefined()
                assertRefused(ids, null)
            }

            DenseUnionVector(allocator, "ids").use { ids ->
                ids.writeObject("ok")
                ids.vectorFor("aliased-uuid", UuidType, false).writeObject(uuid)
                ids.writeUndefined()
                assertRefused(ids, TaggedValue(Keyword.intern("aliased-uuid"), uuid))
            }
        }
    }

    @Test
    fun `delete and erase keep logical reads for encoded unions and wider UUID cells`() {
        RootAllocator().use { allocator ->
            fun assertSourceIids(ids: Vector, expectedIds: List<Any>) {
                val ops = listOf(TxOp.DeleteDocs("public", "docs", ids = ids), TxOp.EraseDocs("public", "docs", ids))
                val bytes = ops.toArrowBytes(allocator)
                Relation.StreamLoader(allocator, Channels.newChannel(bytes.newInput())).use { loader ->
                    Relation(allocator, loader.schema).use { rel ->
                        assertTrue(loader.loadNextPage(rel))
                        val txOps = rel["tx-ops"]
                        for ((row, name) in listOf("delete-docs", "erase-docs").withIndex()) {
                            val iids = txOps.vectorFor(name).vectorFor("iids")
                            assertEquals(expectedIds.size, iids.getListCount(row))
                            val start = iids.getListStartIndex(row)
                            for ((idx, id) in expectedIds.withIndex()) {
                                assertArrayEquals(id.asIid, iids.listElements.getObject(start + idx) as ByteArray)
                            }
                        }
                    }
                }
            }

            val keyword = Keyword.intern("ns/café")
            val type = VectorType.fromLegs(VectorType.I64, VectorType.KEYWORD).runEndEncoded()
            (Vector.open(allocator, "ids", type) as RunEndEncodedVector).use { ids ->
                ids.writeRun(8L, 2)
                ids.writeRun(keyword)
                assertSourceIids(ids, listOf(8L, 8L, keyword))
            }

            val uuid = UUID.fromString("f1234567-89ab-cdef-fedc-ba9876543210")
            UuidVector(FixedSizeBinaryVector(allocator, "ids", false, 24)).use { ids ->
                ids.writeBytes(uuid.asBytes + ByteArray(8) { 42 })
                assertSourceIids(ids, listOf(uuid))
            }
        }
    }

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
