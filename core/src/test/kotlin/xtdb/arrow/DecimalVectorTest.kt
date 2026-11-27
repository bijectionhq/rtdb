package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal

class DecimalVectorTest {
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
    fun `test DecimalVector`() {
        DecimalVector(
            allocator, "dec",
            true, ArrowType.Decimal(5, 2, 32)
        ).use { dec ->

            dec.writeObject(BigDecimal("123.45"))
            dec.writeObject(BigDecimal("789.01"))
            dec.writeNull()

            assertEquals(3, dec.valueCount)
            assertEquals(listOf(BigDecimal("123.45"), BigDecimal("789.01"), null), dec.asList)


            // precision error
            assertThrows<InvalidWriteObjectException> {
                dec.writeObject(BigDecimal("1234567890.01"))
            }

            // scale error
            assertThrows<InvalidWriteObjectException> {
                dec.writeObject(BigDecimal("12.345"))
            }
        }
    }

    @Test
    fun `test different precisions`() {
        DecimalVector(
            allocator, "dec",
            true, ArrowType.Decimal(32, 2, 128)
        ).use { dec ->

            dec.writeObject(BigDecimal("0.01"))
            dec.writeObject(BigDecimal("24580955505371094.01"))

            assertEquals(2, dec.valueCount)
            assertEquals(listOf(BigDecimal("0.01"), BigDecimal("24580955505371094.01")), dec.asList)
        }
    }

    @Test
    fun `identity writes preserve the coefficient and check precision and scale`() {
        val type = ArrowType.Decimal(5, 2, 32)
        DecimalVector(allocator, "src", true, type).use { src ->
            val values = listOf(BigDecimal("-3.75"), BigDecimal("999.99"), null)
            values.forEach(src::writeObject)
            src.setBigDecimal(3, BigDecimal("1000.00"))

            DecimalVector(allocator, "dst", false, type).use { dst ->
                val reader = src.valueReader()
                assertTrue(reader.next().copyIdentityTo(dst))
                assertFalse(dst.nullable)
                dst.writeValue(reader.next())
                dst.writeValue(reader.next())
                assertThrows<InvalidWriteObjectException> { dst.writeValue(reader.next()) }

                assertEquals(values, dst.asList)
            }

            DecimalVector(allocator, "other-scale", false, ArrowType.Decimal(5, 3, 32)).use { dst ->
                val reader = src.valueReader()
                assertThrows<InvalidWriteObjectException> { dst.writeValue(reader.next()) }
            }
        }
    }

    @Test
    fun `decimal identity writes follow selected dense union offsets`() {
        val type = ArrowType.Decimal(5, 2, 32)
        val unionType = VectorType.fromLegs(VectorType.Scalar(type), VectorType.I64)
        Vector.open(allocator, "src", unionType).use { src ->
            val decimals = src.vectorFor(type.toLeg())
            src.writeObject(7L)
            decimals.writeObject(BigDecimal("-3.75"))
            src.writeObject(8L)
            decimals.writeObject(BigDecimal("12.34"))

            DecimalVector(allocator, "dst", false, type).use { dst ->
                val reader = src.select(intArrayOf(3, 1)).valueReader()
                assertTrue(reader.next().copyIdentityTo(dst))
                assertTrue(reader.next().copyIdentityTo(dst))
                assertEquals(listOf(BigDecimal("12.34"), BigDecimal("-3.75")), dst.asList)
            }
        }
    }

    @Test
    fun `test negative scales`() {
        DecimalVector(
            allocator, "dec",
            true, ArrowType.Decimal(32, -2, 128)
        ).use { dec ->

            dec.writeObject(BigDecimal("1E+2"))
            dec.writeObject(BigDecimal("2E+2"))

            assertEquals(2, dec.valueCount)
            assertEquals(-2, (dec.getObject(0) as BigDecimal).scale())
            assertEquals(listOf(BigDecimal("1E+2"), BigDecimal("2E+2")), dec.asList)
        }
    }


}
