@file:JvmName("TxWriter")

package xtdb.tx

import com.google.protobuf.ByteString
import com.google.protobuf.UnsafeByteOperations
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import xtdb.arrow.*
import xtdb.arrow.IID_TYPE
import xtdb.arrow.INSTANT_TYPE
import xtdb.arrow.LIST_TYPE
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.UTF8_TYPE
import xtdb.arrow.VAR_BINARY_TYPE
import xtdb.arrow.VectorType.*
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.TIME_NANO
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.VectorType.Companion.fromLegs
import xtdb.database.DatabaseName
import xtdb.api.error.Incorrect
import xtdb.api.SchemaName
import xtdb.api.TableName
import xtdb.util.asIid
import xtdb.util.writeIid
import java.nio.channels.Channels
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId

private val legacyTxSchema = schema(
    "tx-ops" ofType listTypeOf(fromLegs()),
    "system-time" ofType maybe(INSTANT),
    "default-tz" ofType UTF8,
    "user" ofType maybe(UTF8),
    "user-metadata" ofType Maybe(Struct(emptyMap()))
)

private val flatTxOpsSchema = schema("tx-ops" ofType fromLegs())

private val userMetadataSchema = schema("user-metadata" ofType Maybe(Struct(emptyMap())))

private val FORBIDDEN_SCHEMAS = setOf("xt", "information_schema", "pg_catalog")

private fun checkNotForbidden(schema: SchemaName, table: TableName) {
    if (schema in FORBIDDEN_SCHEMAS) {
        throw Incorrect(
            "Cannot write to table: $schema.$table",
            "xtdb/forbidden-table",
            mapOf("schema" to schema, "table" to table)
        )
    }
}

private fun interface TxOpWriter<O : TxOp> {
    fun writeOp(op: O)
}

private class SqlWriter(val al: BufferAllocator, ops: VectorWriter) {
    private val sqlVec = ops.vectorFor("sql", STRUCT_TYPE, false) as DenseUnionVector.LegVector
    private val queryVec = sqlVec.vectorFor("query", UTF8_TYPE, false)
    private val argsVec = sqlVec.vectorFor("args", VAR_BINARY_TYPE, false).inner as VarBinaryVector

    fun writeOp(op: TxOp.Sql) {
        queryVec.writeObject(op.sql)
        op.args?.let { args ->
            args.openDirectSlice(al).use { argsVec.writeBytes(it.toArrowStream()) }
        }
        sqlVec.endStruct()
    }

    fun writeOp(op: TxOp.SqlBytes) {
        queryVec.writeObject(op.sql)
        if (op.argBytes.isNotEmpty()) argsVec.writeObject(op.argBytes)
        sqlVec.endStruct()
    }
}

private class PutDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.PutDocs> {
    private val putVec = ops.vectorFor("put-docs", STRUCT_TYPE, false)
    private val iidsVec = putVec.vectorFor("iids", LIST_TYPE, false)
    private val iidWriter = iidsVec.getListElements(IID_TYPE, false)
    private val docsVec = putVec.vectorFor("documents", UNION_TYPE, false)
    private val validFromVec = putVec.vectorFor("_valid_from", INSTANT_TYPE, true)
    private val validToVec = putVec.vectorFor("_valid_to", INSTANT_TYPE, true)
    private val tableDocWriters = mutableMapOf<String, VectorWriter>()

    override fun writeOp(op: TxOp.PutDocs) {
        checkNotForbidden(op.schema, op.table)

        val schemaAndTable = "${op.schema}/${op.table}"
        val tableDocsWriter = tableDocWriters.getOrPut(schemaAndTable) {
            docsVec.vectorFor(schemaAndTable, LIST_TYPE, false)
        }

        val tableDocWriter = tableDocsWriter.getListElements(STRUCT_TYPE, false)

        val idVec = op.docs["_id"]
        for (idx in 0 until op.docs.rowCount) {
            idVec.writeIid(idx, iidWriter)
        }
        iidsVec.endList()

        tableDocWriter.append(RelationAsStructReader("docs", op.docs))
        tableDocsWriter.endList()

        op.validFrom?.let { validFromVec.writeObject(it) } ?: validFromVec.writeNull()
        op.validTo?.let { validToVec.writeObject(it) } ?: validToVec.writeNull()

        putVec.endStruct()
    }
}

private class PatchDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.PatchDocs> {
    private val patchVec = ops.vectorFor("patch-docs", STRUCT_TYPE, false)
    private val iidsVec = patchVec.vectorFor("iids", LIST_TYPE, false)
    private val iidWriter = iidsVec.getListElements(IID_TYPE, false)
    private val docsVec = patchVec.vectorFor("documents", UNION_TYPE, false)
    private val validFromVec = patchVec.vectorFor("_valid_from", INSTANT_TYPE, true)
    private val validToVec = patchVec.vectorFor("_valid_to", INSTANT_TYPE, true)
    private val tableDocWriters = mutableMapOf<String, VectorWriter>()

    override fun writeOp(op: TxOp.PatchDocs) {
        checkNotForbidden(op.schema, op.table)

        val schemaAndTable = "${op.schema}/${op.table}"
        val tableDocsWriter = tableDocWriters.getOrPut(schemaAndTable) {
            docsVec.vectorFor(schemaAndTable, LIST_TYPE, false)
        }

        val tableDocWriter = tableDocsWriter.getListElements(STRUCT_TYPE, false)

        val idVec = op.patches["_id"]
        for (idx in 0 until op.patches.rowCount) {
            idVec.writeIid(idx, iidWriter)
        }
        iidsVec.endList()

        tableDocWriter.append(RelationAsStructReader("patches", op.patches))
        tableDocsWriter.endList()

        op.validFrom?.let { validFromVec.writeObject(it) } ?: validFromVec.writeNull()
        op.validTo?.let { validToVec.writeObject(it) } ?: validToVec.writeNull()

        patchVec.endStruct()
    }
}

private fun Vector.supportsSourceIidWrite(): Boolean = when (this) {
    is ByteVector, is ShortVector, is IntVector, is LongVector, is Utf8Vector, is KeywordVector -> true
    is UuidVector -> hasCanonicalWidth
    else -> false
}

private fun VectorReader.writeSourceIids(dest: VectorWriter) {
    val positioned = when (this) {
        is DenseUnionVector -> vectors.all {
            (it is NullVector || it.supportsSourceIidWrite()) && it.name == it.arrowType.toLeg()
        }
        is Vector -> supportsSourceIidWrite()
        else -> false
    }

    for (idx in 0 until valueCount) {
        if (positioned) writeIid(idx, dest)
        else dest.writeBytes(getObject(idx).asIid)
    }
}

private class DeleteDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.DeleteDocs> {
    private val deleteVec = ops.vectorFor("delete-docs", STRUCT_TYPE, false)
    private val tableVec = deleteVec.vectorFor("table", UTF8_TYPE, false)
    private val iidsVec = deleteVec.vectorFor("iids", LIST_TYPE, false)
    private val iidWriter = iidsVec.getListElements(IID_TYPE, false)
    private val validFromVec = deleteVec.vectorFor("_valid_from", INSTANT_TYPE, true)
    private val validToVec = deleteVec.vectorFor("_valid_to", INSTANT_TYPE, true)

    override fun writeOp(op: TxOp.DeleteDocs) {
        checkNotForbidden(op.schema, op.table)

        if (op.ids.valueCount == 0) return

        tableVec.writeObject("${op.schema}/${op.table}")

        op.ids.writeSourceIids(iidWriter)
        iidsVec.endList()

        op.validFrom?.let { validFromVec.writeObject(it) } ?: validFromVec.writeNull()
        op.validTo?.let { validToVec.writeObject(it) } ?: validToVec.writeNull()

        deleteVec.endStruct()
    }
}

private class EraseDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.EraseDocs> {
    private val eraseVec = ops.vectorFor("erase-docs", STRUCT_TYPE, false)
    private val tableVec = eraseVec.vectorFor("table", UTF8_TYPE, false)
    private val iidsVec = eraseVec.vectorFor("iids", LIST_TYPE, false)
    private val iidWriter = iidsVec.getListElements(IID_TYPE, false)

    override fun writeOp(op: TxOp.EraseDocs) {
        checkNotForbidden(op.schema, op.table)

        if (op.ids.valueCount == 0) return

        tableVec.writeObject("${op.schema}/${op.table}")

        op.ids.writeSourceIids(iidWriter)
        iidsVec.endList()

        eraseVec.endStruct()
    }
}

data class TxOpts @JvmOverloads constructor(
    val defaultTz: ZoneId? = null, val systemTime: Instant? = null,
    val user: String? = null, val userMetadata: Map<*, *>? = null,
    // the database to submit to; null means the connection's own database (or the default for a node-level helper).
    // Only meaningful for the autonomous submitTx/executeTx entries - within an open transaction the db is already fixed.
    val dbName: DatabaseName? = null
) {
    fun withFallbackTz(defaultTz: ZoneId?) = if (this.defaultTz != null) this else copy(defaultTz = defaultTz)
}

private fun List<TxOp>.writeOpsInto(al: BufferAllocator, rel: Relation) {
    val txOpVec = rel.vectorFor("tx-ops", UNION_TYPE, false)

    val sqlWriter by lazy { SqlWriter(al, txOpVec) }
    val putDocsWriter by lazy { PutDocsWriter(txOpVec) }
    val patchDocsWriter by lazy { PatchDocsWriter(txOpVec) }
    val deleteDocsWriter by lazy { DeleteDocsWriter(txOpVec) }
    val eraseDocsWriter by lazy { EraseDocsWriter(txOpVec) }

    for (op in this) {
        when (op) {
            is TxOp.PutDocs -> putDocsWriter.writeOp(op)
            is TxOp.PatchDocs -> patchDocsWriter.writeOp(op)
            is TxOp.DeleteDocs -> deleteDocsWriter.writeOp(op)
            is TxOp.EraseDocs -> eraseDocsWriter.writeOp(op)
            is TxOp.Sql -> sqlWriter.writeOp(op)
            is TxOp.SqlBytes -> sqlWriter.writeOp(op)
        }

        if (txOpVec.valueCount > rel.rowCount) rel.endRow()
    }
}

fun List<TxOp>.toArrowBytes(al: BufferAllocator): ByteString =
    Relation(al, flatTxOpsSchema).use { rel ->
        writeOpsInto(al, rel)
        rel.toArrowStream()
    }

@Deprecated("legacy format - use toArrowBytes for the flat protobuf Tx format")
@JvmName("serializeTxOps")
fun List<TxOp>.toLegacyBytes(al: BufferAllocator, opts: TxOpts): ByteArray =
    Relation(al, legacyTxSchema).use { rel ->
        val txOpsVec = rel["tx-ops"]
        val txOpVec = txOpsVec.getListElements(UNION_TYPE, false)

        val sqlWriter by lazy { SqlWriter(al, txOpVec) }
        val putDocsWriter by lazy { PutDocsWriter(txOpVec) }
        val patchDocsWriter by lazy { PatchDocsWriter(txOpVec) }
        val deleteDocsWriter by lazy { DeleteDocsWriter(txOpVec) }
        val eraseDocsWriter by lazy { EraseDocsWriter(txOpVec) }

        for (op in this) {
            when (op) {
                is TxOp.PutDocs -> putDocsWriter.writeOp(op)
                is TxOp.PatchDocs -> patchDocsWriter.writeOp(op)
                is TxOp.DeleteDocs -> deleteDocsWriter.writeOp(op)
                is TxOp.EraseDocs -> eraseDocsWriter.writeOp(op)
                is TxOp.Sql -> sqlWriter.writeOp(op)
                is TxOp.SqlBytes -> sqlWriter.writeOp(op)
            }
        }

        txOpsVec.endList()

        val sysTimeVec = rel["system-time"]
        opts.systemTime?.let { sysTimeVec.writeObject(it) } ?: sysTimeVec.writeNull()

        val defaultTz = checkNotNull(opts.defaultTz) { "missing defaultTz" }
        rel["default-tz"].writeObject(defaultTz.id)

        val userVec = rel["user"]
        opts.user?.let { userVec.writeObject(it) } ?: userVec.writeNull()

        rel["user-metadata"].writeObject(opts.userMetadata)

        rel.endRow()

        rel.toArrowStream().toByteArray()
    }

fun serializeUserMetadata(al: BufferAllocator, metadata: Map<*, *>): ByteString =
    Relation(al, userMetadataSchema).use { rel ->
        rel["user-metadata"].writeObject(metadata)
        rel.endRow()
        rel.toArrowStream()
    }

internal fun <T> withUserMetadata(al: BufferAllocator, bytes: ByteString?, useMetadata: (Vector?) -> T): T {
    if (bytes == null) return useMetadata(null)

    return Channels.newChannel(bytes.newInput()).use { ch ->
        Relation.StreamLoader(al, ch).use { loader ->
            Relation(al, loader.schema).use { rel ->
                loader.loadNextPage(rel)
                useMetadata(rel["user-metadata"])
            }
        }
    }
}

fun deserializeUserMetadata(al: BufferAllocator, bytes: ByteString): Any? =
    withUserMetadata(al, bytes) { it!!.getObject(0) }

private fun VectorReader.isDirectMetadataScalar(): Boolean = when (this) {
    is Utf8Vector, is KeywordVector -> (0 until valueCount).all { UnsafeByteOperations.unsafeWrap(getBytes(it)).isValidUtf8 }
    is Time64Vector -> arrowType == TIME_NANO.arrowType && (0 until valueCount).all {
        getLong(it) in 0L..LocalTime.MAX.toNanoOfDay()
    }
    is TimestampLocalVector -> unit == MICROSECOND
    is TimestampTzVector -> unit == MICROSECOND
    is DurationVector -> unit == MICROSECOND
    is BitVector, is ByteVector, is ShortVector, is IntVector, is LongVector,
    is FloatVector, is DoubleVector, is VarBinaryVector, is UuidVector, is DateDayVector -> true
    else -> false
}

private fun Vector.isDirectMetadata(): Boolean {
    if (valueCount == 0) return this is NullVector

    return when (this) {
        is StructVector -> vectors.all { !it.nullable && it.isDirectMetadata() }
        is ListVector -> listElements.let { elements ->
            getListStartIndex(0) == 0 && getListEndIndex(valueCount - 1) == elements.valueCount &&
                (!elements.nullable || elements.valueCount == 0) && elements.isDirectMetadata()
        }
        is RunEndEncodedVector -> valueCount == 1 && runCount == 1 && runValues.isDirectMetadataScalar()
        else -> isDirectMetadataScalar()
    }
}

private fun Vector.matchesMetadataDestination(dest: VectorWriter?): Boolean {
    if (dest == null) return true
    if (dest.arrowType != arrowType) return false

    return when (this) {
        is StructVector -> dest is StructVector && vectors.all { it.matchesMetadataDestination(dest.vectorForOrNull(it.name)) }
        is ListVector -> dest is ListVector && dest.listElements.let { elements ->
            (elements is NullVector && elements.valueCount == 0) || listElements.matchesMetadataDestination(elements)
        }
        is RunEndEncodedVector -> dest.type.encoding == Encoding.Plain
        else -> true
    }
}

internal fun sourceUserMetadataWriter(source: Vector?): (VectorWriter) -> Unit {
    if (source is StructVector && source.valueCount == 1 && !source.isNull(0)) {
        // A null-only field is omitted by the struct object reader, so it cannot declare a receipt field.
        val children = source.vectors.filterNot { it is NullVector }
        if (children.all { !it.nullable && it.isDirectMetadata() }) {
            return { dest ->
                if (dest is StructVector && children.all { it.matchesMetadataDestination(dest.vectorForOrNull(it.name)) }) {
                    for (child in children) {
                        dest.vectorFor(child.name, child.arrowType, false).appendRange(child, 0, 1)
                    }
                    dest.endStruct()
                } else {
                    dest.writeObject(source.getObject(0))
                }
            }
        }
    }

    val metadata = source?.getObject(0) as? Map<*, *>
    return { it.writeObject(metadata) }
}
