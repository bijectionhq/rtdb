package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.TimeUnit.NANOSECOND
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation.Companion.loader
import xtdb.kw
import xtdb.arrow.VectorType.Companion.asUnionFieldOf
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.SeekableByteChannel
import java.time.DateTimeException
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID

class RelationTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun `bound rows copy matching nested structs with complete reached children`() {
        val nestedType = VectorType.structOf("text" to maybe(VectorType.UTF8), "flag" to VectorType.BOOL)
        val types = linkedMapOf("number" to VectorType.I64, "nested" to nestedType)
        val complete = linkedMapOf("number" to 3L, "nested" to linkedMapOf("text" to "é😀", "flag" to false))
        val sparse = linkedMapOf("number" to 4L, "nested" to linkedMapOf("flag" to true))
        Vector.open(allocator, "arg", VectorType.structOf(types)).use { src ->
            src.writeObject(complete)
            src.writeObject(sparse)
            src.vectorFor("number").writeLong(5L)
            val nested = src.vectorFor("nested")
            nested.vectorFor("text").writeBytes(byteArrayOf(0xff.toByte()))
            nested.vectorFor("flag").writeBoolean(false)
            nested.endStruct()
            src.endStruct()

            Relation(allocator, types).use { dst ->
                assertTrue(dst.tryCopyStructRow(src, 0))
                assertFalse(dst.tryCopyStructRow(src, 1))
                assertFalse(dst.tryCopyStructRow(src, 2))
                assertEquals(1, dst.rowCount)
                assertEquals(listOf(3L), dst["number"].asList)
                dst.writeRow(src.getObject(1) as Map<*, *>)
                dst.writeRow(src.getObject(2) as Map<*, *>)
                assertEquals(listOf(3L, 4L, 5L), dst["number"].asList)
                assertEquals(
                    listOf(complete["nested"], sparse["nested"], mapOf("text" to "\uFFFD", "flag" to false)),
                    dst["nested"].asList
                )
                assertEquals(ByteBuffer.wrap("\uFFFD".toByteArray()), dst["nested"].vectorFor("text").getBytes(2))
            }
        }
    }

    @Test
    fun `bound record copies uuid and date children into matching layouts`() {
        val nestedType = VectorType.structOf("id" to VectorType.UUID, "day" to VectorType.DATE_DAY)
        val recordType = VectorType.structOf(
            "day" to maybe(VectorType.DATE_DAY), "id" to maybe(VectorType.UUID), "nested" to nestedType
        )
        val id = UUID.fromString("fedcba98-7654-3210-8123-456789abcdef")
        val day = LocalDate.ofEpochDay(Int.MIN_VALUE.toLong())
        val nested = mapOf("id" to id, "day" to day)
        val idUnion = VectorType.fromLegs(listOf(VectorType.UUID, VectorType.I64))

        Vector.open(allocator, "src", recordType).use { src ->
            src.writeObject(nested + ("nested" to nested))
            src.writeObject(mapOf("nested" to nested))

            Relation(allocator, mapOf("id" to idUnion, "day" to VectorType.DATE_DAY, "nested" to nestedType)).use { dst ->
                assertTrue(dst.tryCopyStructRow(src, 0))
                assertEquals(idUnion, dst["id"].type)
                assertEquals(VectorType.DATE_DAY, dst["day"].type)
                assertTrue(dst.tryCopyStructRow(src, 1))
                assertEquals(listOf(id, null), dst["id"].asList)
                assertEquals(listOf(day, null), dst["day"].asList)
                assertEquals(listOf(nested, nested), dst["nested"].asList)
            }

            Relation(allocator, mapOf("id" to VectorType.I64, "day" to VectorType.DATE_DAY, "nested" to nestedType)).use { dst ->
                dst.writeRow(mapOf("id" to 7L, "day" to day, "nested" to nested))
                assertFalse(dst.tryCopyStructRow(src, 0))
                assertEquals(1, dst.rowCount)
                assertEquals(1, dst["day"].valueCount)
                dst.writeRow(src.getObject(0) as Map<*, *>)
                assertEquals(listOf(7L, id), dst["id"].asList)
            }
        }
    }

    @Test
    fun `bound temporal record preserves signed ticks and omits null children`() {
        val types = linkedMapOf(
            "timestamp" to VectorType.TIMESTAMP_MICRO,
            "duration" to VectorType.DURATION_MICRO,
            "clock" to maybe(VectorType.TIME_NANO),
        )

        Vector.open(allocator, "src", VectorType.structOf(types)).use { src ->
            src["timestamp"].writeLong(Long.MIN_VALUE)
            src["duration"].writeLong(Long.MAX_VALUE)
            src["clock"].writeLong(LocalTime.MAX.toNanoOfDay())
            src.endStruct()
            src["timestamp"].writeLong(Long.MAX_VALUE)
            src["duration"].writeLong(Long.MIN_VALUE)
            src.endStruct()

            Relation(allocator, types).use { dst ->
                assertTrue(dst.tryCopyStructRow(src, 0))
                assertTrue(dst.tryCopyStructRow(src, 1))
                assertEquals(Long.MIN_VALUE, dst["timestamp"].getLong(0))
                assertEquals(Long.MAX_VALUE, dst["timestamp"].getLong(1))
                assertEquals(Long.MAX_VALUE, dst["duration"].getLong(0))
                assertEquals(Long.MIN_VALUE, dst["duration"].getLong(1))
                assertEquals(LocalTime.MAX.toNanoOfDay(), dst["clock"].getLong(0))
                assertEquals(listOf(LocalTime.MAX, null), dst["clock"].asList)
            }
        }
    }

    @Test
    fun `out of day time ticks retain the object read error`() {
        val types = mapOf("clock" to VectorType.TIME_NANO)
        Vector.open(allocator, "src", VectorType.structOf(types)).use { src ->
            src["clock"].writeLong(-1L)
            src.endStruct()
            src["clock"].writeLong(LocalTime.MAX.toNanoOfDay() + 1)
            src.endStruct()

            Relation(allocator, types).use { dst ->
                for (idx in 0..1) {
                    assertFalse(dst.tryCopyStructRow(src, idx))
                    assertThrows<DateTimeException> { dst["clock"].writeScalarCell(src["clock"], idx) }
                    assertEquals(0, dst["clock"].valueCount)
                    assertEquals(0, dst.rowCount)
                }
            }
        }
    }

    @Test
    fun `bound temporal cells use object conversion across different units or zones`() {
        DurationVector(allocator, "src", false, MICROSECOND).use { src ->
            DurationVector(allocator, "dst", false, NANOSECOND).use { dst ->
                src.writeLong(-1L)
                dst.writeScalarCell(src, 0)
                assertEquals(-1_000L, dst.getLong(0))
            }
        }

        TimestampTzVector(allocator, "src", false, MICROSECOND, ZoneId.of("America/New_York")).use { src ->
            TimestampTzVector(allocator, "dst", false, MICROSECOND, ZoneOffset.UTC).use { dst ->
                src.writeLong(0L)
                assertThrows<InvalidWriteObjectException> { dst.writeScalarCell(src, 0) }
                assertEquals(0, dst.valueCount)
            }
        }
    }

    @Test
    fun testScalarRoundTrip() {
        val buf = ByteArrayOutputStream()

        IntVector.open(allocator, "i32", false).use { i32 ->
            Utf8Vector(allocator, "utf8", true).use { utf8 ->
                val rel = Relation(allocator, listOf(i32, utf8), 0)

                rel.startUnload(Channels.newChannel(buf))
                    .use { unloader ->
                        i32.writeInt(1)
                        utf8.writeObject("Hello")
                        rel.endRow()
                        i32.writeInt(2)
                        utf8.writeNull()
                        rel.endRow()

                        unloader.writePage()
                        rel.clear()

                        i32.writeInt(3)
                        utf8.writeObject("world!")
                        rel.endRow()

                        unloader.writePage()

                        unloader.end()
                    }
            }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            assertEquals(2, loader.pageCount)

            Relation(allocator, loader.schema).use { rel ->
                val i32 = rel["i32"]
                val utf8 = rel["utf8"]

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(1, i32.getInt(0))
                assertEquals("Hello", utf8.getObject(0))
                assertEquals(2, i32.getInt(1))
                assertTrue(utf8.isNull(1))

                loader.loadPage(1, rel)

                assertEquals(1, rel.rowCount)
                assertEquals(3, i32.getInt(0))
                assertEquals("world!", utf8.getObject(0))

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(1, i32.getInt(0))
                assertEquals("Hello", utf8.getObject(0))
                assertEquals(2, i32.getInt(1))
                assertTrue(utf8.isNull(1))
            }
        }
    }

    @Test
    fun testListRoundTrip() {
        val buf = ByteArrayOutputStream()

        val elVector = IntVector.open(allocator, "els", true)

        val list0 = listOf(1, 4, null, 12)
        val list1 = listOf(8)
        val list2 = listOf(1, 0, -1, null)

        ListVector(allocator, "list", false, elVector).use { listVec ->
            val rel = Relation(allocator, listOf(listVec), 0)

            rel.startUnload(Channels.newChannel(buf))
                .use { unloader ->
                    listVec.writeObject(list0)
                    rel.endRow()
                    listVec.writeObject(list1)
                    rel.endRow()

                    unloader.writePage()
                    rel.clear()

                    listVec.writeObject(list2)
                    rel.endRow()

                    unloader.writePage()

                    unloader.end()
                }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            assertEquals(2, loader.pageCount)

            Relation(allocator, loader.schema).use { rel ->
                val listVec = rel["list"]

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(list0, listVec.getObject(0))
                assertEquals(list1, listVec.getObject(1))

                loader.loadPage(1, rel)

                assertEquals(1, rel.rowCount)
                assertEquals(list2, listVec.getObject(0))

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(list0, listVec.getObject(0))
                assertEquals(list1, listVec.getObject(1))
            }
        }
    }

    @Test
    fun testDuvRoundTrip() {
        val duv = DenseUnionVector(
            allocator, "duv",
            listOf(
                IntVector.open(allocator, "i32", false),
                Utf8Vector(allocator, "utf8", true)
            )
        )

        val i32Leg = duv.vectorFor("i32")
        val utf8Leg = duv.vectorFor("utf8")

        i32Leg.writeInt(12)
        utf8Leg.writeObject("hello")
        utf8Leg.writeObject("world!")
        i32Leg.writeInt(34)
        utf8Leg.writeNull()

        val duvValues = listOf(12, "hello", "world!", 34, null)

        assertEquals(duvValues, duv.asList)

        val buf = ByteArrayOutputStream()

        Relation(allocator, listOf(duv), duv.valueCount).use { rel ->
            rel.startUnload(Channels.newChannel(buf)).use { unloader ->
                unloader.writePage()
                unloader.end()
            }
        }

        loader(allocator, buf.toByteArray().asChannel).use { loader ->
            loader.loadPage(0, allocator).use { rel ->
                assertEquals(duvValues, rel["duv"].asList)
            }
        }
    }

    @Test
    fun `a page read that fails mid-channel releases its buffer`() {
        val buf = ByteArrayOutputStream()

        IntVector.open(allocator, "i32", false).use { i32 ->
            val rel = Relation(allocator, listOf(i32), 0)
            rel.startUnload(Channels.newChannel(buf)).use { unloader ->
                i32.writeInt(1)
                rel.endRow()
                unloader.writePage()
                unloader.end()
            }
        }

        val inner = buf.toByteArray().asChannel
        var interrupted = false
        val ch = object : SeekableByteChannel by inner {
            override fun read(dst: ByteBuffer): Int =
                if (interrupted) throw ClosedByInterruptException() else inner.read(dst)
        }

        loader(allocator, ch).use { loader ->
            interrupted = true
            Relation(allocator, loader.schema).use { rel ->
                assertThrows<ClosedByInterruptException> { loader.loadPage(0, rel) }
            }
        }

        assertEquals(0, allocator.allocatedMemory)
    }

    @Test
    fun `fromRecordBatch releases the buffers it loaded when the load throws`() {
        val bytes = Relation(allocator, "i32" ofType VectorType.I32, "i64" ofType VectorType.I64).use { rel ->
            rel.writeRows(mapOf("i32" to 1, "i64" to 2L))
            rel.asArrowFile
        }

        ArrowFileLoader.openFromChannel(allocator, bytes.asChannel).use { loader ->
            loader.openPage(0).use { rb ->
                val i32Only = schema("i32" ofType VectorType.I32)
                assertThrows<IllegalArgumentException> { Relation.fromRecordBatch(allocator, i32Only, rb) }
            }
        }

        assertEquals(0, allocator.allocatedMemory)
    }

    @Test
    fun testStreamRoundTrip() {
        val row1 = mapOf("i32" to 4)
        val row2 = mapOf("i32" to 8)

        val bytes = Relation(allocator, "i32" ofType VectorType.I32).use { rel ->
            rel.writeRows(row1, row2)
            rel.asArrowStream
        }

        val rows = Relation.StreamLoader(allocator, bytes.asChannel).use { loader ->
            Relation(allocator, loader.schema).use { rel ->
                assertTrue(loader.loadNextPage(rel))

                rel.toMaps(SNAKE_CASE_STRING)
                    .also {
                        assertFalse(loader.loadNextPage(rel))
                    }
            }
        }

        assertEquals(listOf(row1, row2), rows)
    }

    @Test
    fun testPromotion() {
        Relation(allocator).use { rel ->
            val intVec = rel.vectorFor("foo", I32_TYPE, false)
            intVec.writeInt(32)
            rel.endRow()

            assertEquals(listOf(mapOf("foo".kw to 32)), rel.asMaps)

            val strVec = rel.vectorFor("bar", UTF8_TYPE, false)
            strVec.writeObject("hello")
            rel.endRow()

            assertEquals(
                schema("foo" ofType maybe(VectorType.I32), "bar" ofType maybe(VectorType.UTF8)),
                rel.schema
            )

            assertEquals(
                listOf(
                    mapOf("foo".kw to 32),
                    mapOf("bar".kw to "hello")
                ),
                rel.asMaps
            )
        }
    }

    @Test
    fun testWriteRows() {
        val rows = listOf(
            mapOf("foo" to 32),
            mapOf("foo" to 64, "bar" to "hello"),
            mapOf("bar" to listOf("a", "promotion"))
        )
        Relation.openFromRows(allocator, rows).use { rel ->
            assertEquals(
                schema(
                    "foo" ofType maybe(VectorType.I32),
                    "bar".asUnionFieldOf(
                        "utf8" to maybe(VectorType.UTF8),
                        "list" to listTypeOf(VectorType.UTF8)
                    )
                ),
                rel.schema
            )

            assertEquals(rows, rel.toMaps(SNAKE_CASE_STRING))
        }
    }

    // xtdb.expression emits a read that faults on a bottom-typed column, relying on this to keep it
    // unreachable - so a change to this padding is not a local change (#5948).
    @Test
    fun `endRows takes a short column off the lattice bottom`() {
        Relation(allocator, listOf(NullVector("a", false)), 0).use { rel ->
            assertEquals(VectorType.Nothing, rel["a"].type)

            rel.endRows(1)
            assertEquals(VectorType.Null, rel["a"].type, "padding writes a real null")
        }
    }
}
