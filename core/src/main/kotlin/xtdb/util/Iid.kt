@file:JvmName("Iid")
package xtdb.util

import clojure.lang.Keyword
import com.google.protobuf.UnsafeByteOperations
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.InternalApi
import xtdb.asBytes
import xtdb.api.error.Incorrect
import xtdb.arrow.I16_TYPE
import xtdb.arrow.I32_TYPE
import xtdb.arrow.I64_TYPE
import xtdb.arrow.I8_TYPE
import xtdb.arrow.UTF8_TYPE
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.arrow.extensions.UuidType
import xtdb.arrow.toLeg
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID

private val messageDigest = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

/** @suppress */
@InternalApi
fun iidFromLong(id: Long) = messageDigest.get().digest("i$id".toByteArray()).copyOfRange(0, 16)

/** @suppress */
@InternalApi
fun iidFromUtf8OrNull(bytes: ByteBuffer): ByteArray? {
    if (!UnsafeByteOperations.unsafeWrap(bytes).isValidUtf8) return null
    return messageDigest.get().run {
        update('s'.code.toByte())
        update(bytes.duplicate())
        digest().copyOfRange(0, 16)
    }
}

private fun VectorReader.idType(idx: Int): ArrowType {
    val type = arrowType
    if (type !is ArrowType.Union) return type
    val leg = getLeg(idx) ?: return type
    val legType = vectorForOrNull(leg)?.arrowType ?: return type
    return if (legType.toLeg() == leg) legType else type
}

@OptIn(InternalApi::class)
private fun VectorReader.typedIid(idx: Int): ByteArray? {
    if (isNull(idx)) return null
    return when (idType(idx)) {
        UuidType -> ByteArray(16).also { getBytes(idx).duplicate().get(it) }
        I8_TYPE -> iidFromLong(getByte(idx).toLong())
        I16_TYPE -> iidFromLong(getShort(idx).toLong())
        I32_TYPE -> iidFromLong(getInt(idx).toLong())
        I64_TYPE -> iidFromLong(getLong(idx))
        UTF8_TYPE -> iidFromUtf8OrNull(getBytes(idx))
        else -> null
    }
}

/** @suppress */
@InternalApi
fun iidFromId(ids: VectorReader, idx: Int): ByteArray = ids.typedIid(idx) ?: ids.getObject(idx).asIid

/** @suppress */
@InternalApi
fun iidFromIdOrNull(ids: VectorReader, idx: Int): ByteArray? = ids.typedIid(idx) ?: when (val id = ids.getObject(idx)) {
    is UUID, is String, is Keyword, is Byte, is Short, is Int, is Long -> id.asIid
    else -> null
}

@OptIn(InternalApi::class)
internal fun VectorReader.writeIid(idx: Int, dest: VectorWriter) {
    if (!isNull(idx) && idType(idx) == UuidType) dest.writeBytes(getBytes(idx))
    else dest.writeBytes(iidFromId(this, idx))
}

@OptIn(InternalApi::class)
val Any?.asIid: ByteArray
    get() = when (this) {
        is UUID -> this.asBytes
        is Int, is Long, is Short, is Byte -> iidFromLong((this as Number).toLong())
        else -> {
            val eidBytes = when (this) {
                is String -> "s$this".toByteArray()
                is Keyword -> "k$this".toByteArray()
                else -> throw Incorrect(
                    "Invalid ID type: ${this?.javaClass?.name}",
                    "xtdb/invalid-id",
                    mapOf("type" to this?.javaClass?.name, "eid" to this)
                )
            }
            messageDigest.get().digest(eidBytes).copyOfRange(0, 16)
        }
    }
