package xtdb.arrow

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.PersistentArrayMap
import com.google.protobuf.UnsafeByteOperations
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.kw
import java.nio.ByteBuffer

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

internal fun VectorReader.isAscii(idx: Int): Boolean {
    val bytes = getBytes(idx)
    return (bytes.position() until bytes.limit()).all { bytes.get(it) >= 0 }
}

internal fun VectorReader.hasValidUtf8(idx: Int) = UnsafeByteOperations.unsafeWrap(getBytes(idx)).isValidUtf8

internal fun VectorWriter.scalarObjectTarget(src: VectorReader, idx: Int): VectorWriter? {
    val sourceType = src.arrowType
    if (src.type.encoding != Encoding.Plain) return null
    if (!sourceType.hasScalarObjectIdentity() &&
        !(sourceType == UTF8_TYPE && (src.isNull(idx) || src.hasValidUtf8(idx)))) return null

    val target = if (this is DenseUnionVector) vectorForOrNull(sourceType.toLeg()) else this
    return target?.takeIf { it.arrowType == sourceType }
}

internal fun VectorWriter.copyNonNullScalar(src: VectorReader, idx: Int) {
    if (src.nullable && !nullable) writeValue(src.valueReader().also { it.pos = idx })
    else src.rowCopier(this).copyRow(idx)
}

fun VectorWriter.writeScalarCell(src: VectorReader, idx: Int) {
    val target = scalarObjectTarget(src, idx)
    if (target == null) writeObject(src.getObject(idx))
    else if (src.isNull(idx)) writeObject(null)
    else target.copyNonNullScalar(src, idx)
}
