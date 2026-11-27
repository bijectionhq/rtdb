package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.assertThrows
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.extensions.IntervalMDMType
import xtdb.test.AllocatorResolver
import xtdb.time.Interval
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.ZoneId

@ExtendWith(AllocatorResolver::class)
class FixedWidthValueReaderTest {
    private val rowCount = 200

    @Test
    fun `positioned interval writes preserve signed components and physical units`(al: BufferAllocator) {
        val cases = listOf(
            MinorType.INTERVALYEAR.type to Interval(Int.MIN_VALUE, 0, 0),
            MinorType.INTERVALDAY.type to Interval(0, Int.MAX_VALUE, Int.MIN_VALUE.toLong() * 1_000_000),
            MinorType.INTERVALMONTHDAYNANO.type to Interval(Int.MIN_VALUE, Int.MAX_VALUE, Long.MIN_VALUE),
            IntervalMDMType to Interval(Int.MAX_VALUE, Int.MIN_VALUE, Long.MAX_VALUE)
        )
        for ((type, value) in cases) {
            Vector.open(al, "src", VectorType.Scalar(type)).use { src ->
                src.writeObject(Interval.ZERO)
                src.writeNull()
                src.writeObject(value)

                Vector.open(al, "dst", VectorType.Scalar(type)).use { dst ->
                    val rows = src.select(intArrayOf(2, 1, 0)).valueReader()
                    assertTrue(rows.next().copyIdentityTo(dst))
                    assertFalse(dst.nullable)
                    dst.writeValue(rows.next())
                    dst.writeValue(object : ValueReader by rows.next() {
                        override fun readObject(): Any? = error("unexpected interval decoding")
                    })
                    assertEquals(listOf(value, null, Interval.ZERO), dst.asList)
                }
            }
        }
    }

    @Test
    fun `interval layout changes retain the object writer's validation`(al: BufferAllocator) {
        IntervalMonthDayMicroVector(IntervalMonthDayNanoVector(al, "src", false)).use { src ->
            src.writeObject(Interval(0, 1, 1))
            val row = src.valueReader().next()

            IntervalMonthDayNanoVector(al, "nanos", false).use { dst ->
                assertFalse(row.copyIdentityTo(dst))
                dst.writeValue(row)
                assertEquals(listOf(Interval(0, 1, 1)), dst.asList)
            }
            IntervalDayTimeVector(al, "millis", false).use { dst ->
                assertFalse(row.copyIdentityTo(dst))
                assertThrows<InvalidWriteObjectException> { dst.writeValue(row) }
                assertEquals(0, dst.valueCount)
            }
        }
    }

    private fun isNullRow(idx: Int) = idx % 7 == 3 || idx == 63 || idx == 64

    private fun rawValue(width: Int, idx: Int): Long = when (width) {
        1 -> idx.toLong()
        2 -> (idx + 1) * 7L
        4 -> (idx + 1) * 301L
        else -> (idx + 1) * 86_400_123L
    }

    private fun FixedWidthVector.writeRow(idx: Int) {
        val raw = rawValue(byteWidth, idx)
        when (this) {
            is DecimalVector -> writeObject(BigDecimal.valueOf(raw, 2))
            is IntervalDayTimeVector -> writeObject(Interval(0, idx, (raw % 86_400_000) * 1_000_000))
            is IntervalMonthDayNanoVector -> writeObject(Interval(idx, idx + 1, raw))
            is FixedSizeBinaryVector -> writeBytes(ByteBuffer.allocate(byteWidth).putLong(0, raw))
            is FloatVector -> writeFloat(raw.toFloat())
            is DoubleVector -> writeDouble(raw.toDouble())
            else -> when (byteWidth) {
                1 -> writeByte(raw.toByte())
                2 -> writeShort(raw.toShort())
                4 -> writeInt(raw.toInt())
                else -> writeLong(raw)
            }
        }
    }

    private fun ownRead(vec: FixedWidthVector): (ValueReader) -> Any? = when (vec) {
        is DecimalVector, is FixedSizeBinaryVector, is IntervalDayTimeVector, is IntervalMonthDayNanoVector -> { _ -> null }
        is FloatVector -> { r -> r.readFloat() }
        is DoubleVector -> { r -> r.readDouble() }
        else -> when (vec.byteWidth) {
            1 -> { r -> r.readByte() }
            2 -> { r -> r.readShort() }
            4 -> { r -> r.readInt() }
            else -> { r -> r.readLong() }
        }
    }

    private fun FixedWidthVector.fill(withNulls: Boolean) = apply {
        repeat(rowCount) { idx -> if (withNulls && isNullRow(idx)) writeNull() else writeRow(idx) }
    }

    private val vectors: Map<String, (BufferAllocator, Boolean) -> FixedWidthVector> = mapOf(
        "long" to { al, n -> LongVector(al, "v", n) },
        "int" to { al, n -> IntVector.open(al, "v", n) },
        "short" to { al, n -> ShortVector(al, "v", n) },
        "byte" to { al, n -> ByteVector(al, "v", n) },
        "double" to { al, n -> DoubleVector(al, "v", n) },
        "float" to { al, n -> FloatVector(al, "v", n) },
        "decimal" to { al, n -> DecimalVector(al, "v", n, ArrowType.Decimal(38, 2, 128)) },
        "date-day" to { al, n -> DateDayVector(al, "v", n) },
        "date-milli" to { al, n -> DateMilliVector(al, "v", n) },
        "time32" to { al, n -> Time32Vector(al, "v", n, TimeUnit.SECOND) },
        "time64" to { al, n -> Time64Vector(al, "v", n, TimeUnit.NANOSECOND) },
        "timestamp" to { al, n -> TimestampLocalVector(al, "v", n, TimeUnit.MICROSECOND) },
        "timestamp-tz" to { al, n -> TimestampTzVector(al, "v", n, TimeUnit.MICROSECOND, ZoneId.of("Europe/London")) },
        "duration" to { al, n -> DurationVector(al, "v", n, TimeUnit.MICROSECOND) },
        "fixed-size-binary" to { al, n -> FixedSizeBinaryVector(al, "v", n, 16) },
        "interval-year-month" to { al, n -> IntervalYearMonthVector(al, "v", n) },
        "interval-day-time" to { al, n -> IntervalDayTimeVector(al, "v", n) },
        "interval-month-day-nano" to { al, n -> IntervalMonthDayNanoVector(al, "v", n) },
    )

    private fun outcome(read: () -> Any?): Any? =
        runCatching(read).fold(
            { if (it is ByteArray) it.toList() else it },
            { it::class }
        )

    private fun reads(rdr: ValueReader, ownRead: (ValueReader) -> Any?): List<Any?> = listOf(
        outcome { rdr.isNull },
        outcome { rdr.leg },
        outcome { rdr.readBoolean() },
        outcome { ownRead(rdr) },
        outcome { rdr.readAsLong() },
        outcome { rdr.readAsDouble() },
        outcome { rdr.readBytes() },
        outcome { rdr.readObject() },
    )

    private fun checkRow(vec: FixedWidthVector, rdr: VectorReader, expected: ValueReader, row: RowValueReader, what: String) {
        expected.pos = row.pos
        val ownRead = ownRead(vec)
        assertEquals(reads(expected, ownRead), reads(row, ownRead), "$what, row ${row.pos}")
        assertEquals(outcome { rdr.getObject(row.pos, SNAKE_CASE_STRING) }, outcome { row.readObject(SNAKE_CASE_STRING) }, "$what, row ${row.pos}")
    }

    private fun shapes(vec: VectorReader) = mapOf(
        "direct" to vec,
        "select(IntArray)" to vec.select(IntArray(150) { (it * 37) % rowCount }),
        "select(start, len)" to vec.select(60, 90),
        "slice of a selection" to vec.select(IntArray(150) { (it * 11) % rowCount }).select(30, 100),
        "selection of a slice" to vec.select(10, 150).select(IntArray(80) { (it * 13) % 150 }),
    )

    @Test
    fun `rows read as the vector's getX and getObject, for every fixed-width vector with a word-cached reader`(al: BufferAllocator) {
        vectors.forEach { (label, open) ->
            listOf(true, false).forEach { withNulls ->
                open(al, withNulls).fill(withNulls).use { vec ->
                    shapes(vec).forEach { (shape, rdr) ->
                        val what = "$label, nulls=$withNulls, $shape"
                        val expected = ValueReader.ForVector(rdr)
                        var count = 0
                        rdr.valueReader().forEach { row ->
                            assertEquals(count++, row.pos, what)
                            checkRow(vec, rdr, expected, row, what)
                        }
                        assertEquals(rdr.valueCount, count, what)
                    }
                }
            }
        }
    }

    @Test
    fun `seeking across a validity word reads the row sought`(al: BufferAllocator) {
        val seeks = listOf(0, 63, 64, 130, 65, 62, 5, 199, 128, 127, 64, 63)

        vectors.forEach { (label, open) ->
            open(al, true).fill(true).use { vec ->
                mapOf(
                    "direct" to vec,
                    "reversed selection" to vec.select(IntArray(rowCount) { rowCount - 1 - it }),
                    "slice" to vec.select(0, rowCount),
                ).forEach { (shape, rdr) ->
                    val what = "$label, $shape"
                    val expected = ValueReader.ForVector(rdr)
                    val row = rdr.valueReader()
                    seeks.forEach { idx ->
                        row.pos = idx
                        checkRow(vec, rdr, expected, row, "$what, seek to $idx")
                    }

                    row.pos = 62
                    checkRow(vec, rdr, expected, row.next(), "$what, next after seek")
                    checkRow(vec, rdr, expected, row.next(), "$what, next after seek")
                    assertEquals(64, row.pos, what)
                }
            }
        }
    }

    @Test
    fun `a validity buffer shorter than a word reads its nulls`(al: BufferAllocator) {
        val rows = 10
        LongVector(al, "v", true).use { vec ->
            al.buffer(2).use { validity ->
                al.buffer(rows * 8L).use { data ->
                    validity.setZero(0, validity.capacity())
                    repeat(rows) { idx ->
                        if (idx != 3 && idx != 9) validity.setByte(idx / 8L, validity.getByte(idx / 8L).toInt() or (1 shl (idx % 8)))
                        data.setLong(idx * 8L, idx * 10L)
                    }
                    vec.loadPage(mutableListOf(ArrowFieldNode(rows.toLong(), 2)), mutableListOf(validity, data))
                }
            }

            assertEquals(2L, vec.validityBuffer!!.buf.capacity())
            assertEquals(
                List(rows) { if (it == 3 || it == 9) null else it * 10L },
                vec.valueReader().asSequence().map { if (it.isNull) null else it.readLong() }.toList()
            )
        }
    }

    @Test
    fun `a value reader reads its rows while the vector grows under it`(al: BufferAllocator) {
        LongVector(al, "v", true).use { vec ->
            repeat(10) { if (it == 4) vec.writeNull() else vec.writeLong(it.toLong()) }

            val rows = vec.valueReader()
            val seen = mutableListOf<Long?>()
            repeat(3) { rows.next().let { seen += if (it.isNull) null else it.readLong() } }

            repeat(100_000) { vec.writeLong(-1L) }

            rows.forEach { seen += if (it.isNull) null else it.readLong() }
            assertEquals(List(10) { if (it == 4) null else it.toLong() }, seen)
            assertFalse(rows.hasNext())
        }
    }

    @Test
    fun `a seek reads the vector as it is now, after writes since the reader was made`(al: BufferAllocator) {
        LongVector(al, "v", true).use { vec ->
            repeat(10) { vec.writeLong(it.toLong()) }

            val row = vec.valueReader()
            row.pos = 5
            assertFalse(row.isNull)

            vec.setNull(6)
            row.pos = 6
            assertEquals(true, row.isNull, "a row nulled within the cached word")

            repeat(100) { vec.writeLong(it + 10L) }
            vec.setNull(70)
            row.pos = 70
            assertEquals(true, row.isNull, "an appended row, past the reader's row count")
            row.pos = 71
            assertEquals(71L to false, row.readLong() to row.isNull, "an appended row, past the reader's row count")
        }

        LongVector(al, "v", false).use { vec ->
            repeat(10) { vec.writeLong(it.toLong()) }

            val row = vec.valueReader()
            row.pos = 2
            assertFalse(row.isNull)

            vec.writeNull()
            row.pos = 10
            assertEquals(true, row.isNull, "a vector made nullable since")
            row.pos = 2
            assertEquals(2L to false, row.readLong() to row.isNull, "a vector made nullable since")
        }
    }
}
