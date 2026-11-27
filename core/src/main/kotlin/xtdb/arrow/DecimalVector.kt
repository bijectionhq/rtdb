package xtdb.arrow

import com.carrotsearch.hppc.DoubleArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.KEBAB_CASE_KEYWORD
import xtdb.arrow.agg.VectorSummer
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.api.error.Unsupported
import xtdb.hash.addDoubles
import xtdb.util.Hasher
import java.math.BigDecimal
import java.nio.ByteOrder

private const val DECIMAL_ERROR_KEY = "xtdb.error/decimal-error"
private val LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN

class DecimalVector private constructor(
    override val al: BufferAllocator,
    override var name: String, override var valueCount: Int,
    override var validityBuffer: BitBuffer?, override val dataBuffer: ExtensibleBuffer,
    private val decimalType: Decimal
) : NumericVector(), MetadataFlavour.Number {

    companion object {
        private val BIT_WIDTHS = setOf(32, 64, 128, 256)
    }

    val precision = decimalType.precision
    val scale = decimalType.scale
    val bitWidth = decimalType.bitWidth

    init {
        require(bitWidth in BIT_WIDTHS) { "Invalid bit width for DecimalVector: $bitWidth" }
    }

    constructor(al: BufferAllocator, name: String, nullable: Boolean, decimalType: Decimal)
            : this(al, name, 0, if (nullable) BitBuffer(al) else null, ExtensibleBuffer(al), decimalType)

    override val byteWidth = (bitWidth / 8)

    override val arrowType: ArrowType = Decimal(precision, scale, bitWidth)

    fun getDecimal(idx: Int) = dataBuffer.readBigDecimal(idx, scale, byteWidth)

    override fun getAsFloat(idx: Int): Float = getDecimal(idx).toFloat()
    override fun getAsDouble(idx: Int): Double = getDecimal(idx).toDouble()

    override fun setBigDecimal(idx: Int, v: BigDecimal) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        dataBuffer.setBigDecimal(idx, v, byteWidth)
        valueCount = maxOf(valueCount, idx + 1)
    }

    fun increment(idx: Int, v: BigDecimal) {
        ensureCapacity(idx + 1)
        setNotNull(idx)
        val result = if (isNull(idx)) v else getDecimal(idx).add(v)
        dataBuffer.setBigDecimal(idx, result, byteWidth)
        valueCount = maxOf(valueCount, idx + 1)
    }

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>): BigDecimal = getDecimal(idx)

    override fun writeObject0(value: Any) {
        if (value is BigDecimal) {
            if (value.precision() > precision || value.scale() != scale) {
                throw InvalidWriteObjectException(this, value)
            }

            // HACK, we throw unsupported here, but it should likely be dealt with in the EE if an object doesn't fit
            try {
                dataBuffer.writeBigDecimal(value, byteWidth)
            } catch (e: UnsupportedOperationException) {
                throw Unsupported(e.message, DECIMAL_ERROR_KEY, cause = e)
            }
            writeNotNull()
        } else throw InvalidWriteObjectException(this, value)
    }

    internal fun tryCopyValue(src: DecimalVector, idx: Int): Boolean {
        if (arrowType != src.arrowType || precision < 1) return false

        // A coefficient fitting in 3p magnitude bits is smaller than 10^p, including the negative boundary.
        val signBit = minOf(bitWidth - 1L, 3L * precision).toInt()
        val signByte = signBit / Byte.SIZE_BITS
        val offset = idx.toLong() * byteWidth
        val srcBuffer = src.dataBuffer
        fun readByte(i: Int): Int {
            val physicalIndex = if (LITTLE_ENDIAN) i else byteWidth - 1 - i
            return srcBuffer.getByte(offset + physicalIndex).toInt()
        }

        val sign = readByte(signByte) shr (signBit % Byte.SIZE_BITS)
        if (sign != 0 && sign != -1) return false
        for (i in signByte + 1 until byteWidth) {
            if (readByte(i) != sign) return false
        }

        dataBuffer.writeBytes(srcBuffer, offset, byteWidth.toLong())
        writeNotNull()
        return true
    }

    override fun writeValue0(v: ValueReader) {
        if (!v.copyIdentityTo(this)) writeObject(v.readObject())
    }

    override fun sumInto(outVec: Vector): VectorSummer {
        if (outVec !is DecimalVector) return super.sumInto(outVec)

        return VectorSummer { idx, groupIdx ->
            outVec.increment(groupIdx, if (isNull(idx)) BigDecimal.ZERO else getDecimal(idx))
        }
    }

    override fun hashCode0(idx: Int, hasher: Hasher) = hasher.hash(getObject0(idx, KEBAB_CASE_KEYWORD).toDouble())

    override fun openSlice(al: BufferAllocator) =
        DecimalVector(al, name, valueCount, validityBuffer?.openSlice(al), dataBuffer.openSlice(al), decimalType)

    override fun doublesTo(out: DoubleArrayList, sel: IntArray?) = out.addDoubles(valueCount, sel) { if (isNull(it)) 0.0 else getDecimal(it).toDouble() }
}
