package xtdb.arrow

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.PersistentArrayMap
import com.google.protobuf.UnsafeByteOperations
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.InternalApi
import xtdb.arrow.extensions.UuidType
import xtdb.kw
import java.nio.ByteBuffer
import java.time.LocalTime

internal data class InvalidWriteObjectException(
    val vec: VectorWriter, val obj: Any?
) : IllegalStateException("invalid writeObject"), IExceptionInfo {
    override fun getData(): IPersistentMap =
        PersistentArrayMap.create(mapOf("target".kw to vec::class.simpleName, "obj".kw to obj))
}

private val VectorReader.typeAsString get() =
    "${if (nullable) "?" else ""}$arrowType"

internal data class InvalidCopySourceException(
    val src: VectorReader, val dest: VectorWriter
) : IllegalStateException("illegal copy src vector: ${src.typeAsString} -> ${dest.typeAsString}")

interface VectorWriter : VectorReader, AutoCloseable {

    override val nullable: Boolean

    fun writeUndefined()
    fun writeNull()

    fun writeBoolean(v: Boolean): Unit = unsupported("writeBoolean")
    fun writeByte(v: Byte): Unit = unsupported("writeByte")
    fun writeShort(v: Short): Unit = unsupported("writeShort")
    fun writeInt(v: Int): Unit = unsupported("writeInt")
    fun writeLong(v: Long): Unit = unsupported("writeLong")
    fun writeFloat(v: Float): Unit = unsupported("writeFloat")
    fun writeDouble(v: Double): Unit = unsupported("writeDouble")
    fun writeBytes(v: ByteBuffer): Unit = unsupported("writeBytes")
    fun writeBytes(v: ByteArray) = writeBytes(ByteBuffer.wrap(v))
    fun writeLongLong(high: Long, low: Long): Unit = unsupported("writeLongLong")
    fun writeObject(obj: Any?)

    fun writeValue(v: ValueReader) = if (v.isNull) writeNull() else writeValue0(v)
    fun writeValue0(v: ValueReader)

    override fun vectorForOrNull(name: String): VectorWriter? = unsupported("vectorFor")

    override fun vectorFor(name: String): VectorWriter = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun get(name: String) = vectorFor(name)

    /**
     * @return a vector with the given name, creating/promoting if necessary
     */
    fun vectorFor(name: String, arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("vectorFor")

    fun endStruct(): Unit = unsupported("endStruct")

    override val listElements: VectorWriter get() = unsupported("listElements")
    fun getListElements(arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("getListElements")
    fun endList(): Unit = unsupported("endList")

    override val mapKeys: VectorWriter get() = unsupported("mapKeys")
    fun getMapKeys(arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("mapKeys")
    override val mapValues: VectorWriter get() = unsupported("mapValues")
    fun getMapValues(arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("getMapValues")

    fun clear()

    fun writeAll(vals: Iterable<Any?>) = apply { vals.forEach { writeObject(it) } }

    fun append(r: VectorReader) = appendRange(r, 0, r.valueCount)

    fun appendRows(r: VectorReader, sel: IntArray) = r.rowCopier(this).copyRows(sel)

    fun appendRange(r: VectorReader, startIdx: Int, length: Int) =
        r.rowCopier(this).copyRange(startIdx, length)
}

internal fun ArrowType.hasScalarObjectIdentity() = when (this) {
    BOOL_TYPE, I8_TYPE, I16_TYPE, I32_TYPE, I64_TYPE, F32_TYPE, F64_TYPE, VAR_BINARY_TYPE -> true
    else -> false
}

/** @suppress */
@InternalApi
fun VectorWriter.acceptsScalarProjection(resultType: VectorType, canonicalUuid: Boolean): Boolean =
    this is Vector && arrowType == resultType.arrowType && when (this) {
        is DateDayVector -> resultType.encoding == Encoding.Plain
        is UuidVector -> resultType.encoding == Encoding.Plain && canonicalUuid && hasCanonicalWidth
        else -> arrowType.hasScalarObjectIdentity()
    }

/** @suppress */
@InternalApi
fun VectorWriter.acceptsListProjection(resultType: VectorType): Boolean {
    if (this !is ListVector || !hasPrimitiveElements) return false
    val result = if (resultType is VectorType.Maybe) resultType.mono else resultType
    return monoType == result
}

internal fun ByteBuffer.isWellFormedUtf8() = UnsafeByteOperations.unsafeWrap(this).isValidUtf8

internal fun VectorReader.hasCanonicalTemporalLayout() = when (this) {
    is TimestampLocalVector -> unit == MICROSECOND
    is TimestampTzVector -> unit == MICROSECOND
    is DurationVector -> unit == MICROSECOND
    is Time64Vector -> arrowType == VectorType.TIME_NANO.arrowType
    else -> false
}

internal fun Long.isNanoTimeOfDay() = this in 0L..LocalTime.MAX.toNanoOfDay()

private tailrec fun unwrapCellSource(reader: VectorReader): VectorReader = when (val source = VectorReader.unwrapRenamed(reader)) {
    is IndirectVector -> unwrapCellSource(source.inner)
    else -> source
}

internal fun VectorWriter.scalarObjectTarget(src: VectorReader, idx: Int): VectorWriter? {
    val sourceType = src.arrowType
    if (src.type.encoding != Encoding.Plain) return null
    val hasIdentity = when (sourceType) {
        VectorType.DATE_DAY.arrowType -> unwrapCellSource(src) is DateDayVector
        UuidType -> (unwrapCellSource(src) as? UuidVector)?.hasCanonicalWidth == true
        UTF8_TYPE -> src.isNull(idx) || src.getBytes(idx).isWellFormedUtf8()
        is ArrowType.Timestamp, is ArrowType.Duration, is ArrowType.Time -> unwrapCellSource(src).let { source ->
            source.hasCanonicalTemporalLayout() &&
                (source !is Time64Vector || src.isNull(idx) || src.getLong(idx).isNanoTimeOfDay())
        }
        else -> sourceType.hasScalarObjectIdentity()
    }
    if (!hasIdentity) return null

    val target = (if (this is DenseUnionVector) vectorForOrNull(sourceType.toLeg()) else this) ?: return null
    if (target.arrowType != sourceType) return null
    val storage = if (target is DenseUnionVector.LegVector) target.inner else target
    when (sourceType) {
        UuidType -> if (storage !is UuidVector || !storage.hasCanonicalWidth) return null
        is ArrowType.Timestamp, is ArrowType.Duration, is ArrowType.Time ->
            if (!storage.hasCanonicalTemporalLayout()) return null
    }
    return target
}

internal fun VectorWriter.tableCellObjectTarget(src: VectorReader, idx: Int): VectorWriter? {
    if (src.arrowType != LIST) return scalarObjectTarget(src, idx)
    if (unwrapCellSource(src) !is ListVector) return null

    val target = (if (this is DenseUnionVector) vectorForOrNull(LIST.toLeg()) else this) ?: return null
    val storage = if (target is DenseUnionVector.LegVector) target.inner else target
    return if (storage is ListVector && storage.acceptsPrimitiveCell(src, idx)) target else null
}

internal fun VectorWriter.copyNonNullCell(src: VectorReader, idx: Int) {
    if (src.arrowType != LIST && src.nullable && !nullable) writeValue(src.valueReader().also { it.pos = idx })
    else src.rowCopier(this).copyRow(idx)
}

fun VectorWriter.writeScalarCell(src: VectorReader, idx: Int) {
    val target = scalarObjectTarget(src, idx)
    if (target == null) writeObject(src.getObject(idx))
    else if (src.isNull(idx)) writeObject(null)
    else target.copyNonNullCell(src, idx)
}

/** @suppress */
@InternalApi
fun VectorWriter.writeTableCell(src: VectorReader, idx: Int) {
    val target = tableCellObjectTarget(src, idx)
    if (target == null) writeObject(src.getObject(idx))
    else if (src.isNull(idx)) writeObject(null)
    else target.copyNonNullCell(src, idx)
}
