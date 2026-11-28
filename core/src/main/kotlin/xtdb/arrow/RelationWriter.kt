package xtdb.arrow

import clojure.lang.Keyword
import clojure.lang.Symbol
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.InternalApi
import xtdb.util.closeAll
import xtdb.util.normalForm

interface RelationWriter : RelationReader {

    override var rowCount: Int

    override val vectors: Collection<VectorWriter>
    override fun vectorForOrNull(name: String): VectorWriter?
    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    fun vectorFor(name: String, arrowType: ArrowType, nullable: Boolean): VectorWriter = unsupported("vectorFor/2")
    override fun get(name: String) = vectorFor(name)

    fun endRows(rows: Int)
    fun endRow() = endRows(1)

    fun append(rel: RelationReader) {
        rel.vectors.forEach { vectorFor(it.name, it.arrowType, it.nullable).append(it) }
        rowCount += rel.rowCount
    }

    @OptIn(InternalApi::class)
    fun writeRow(row: Map<*, *>) {
        for ((k, v) in row) {
            val normalKey = when (k) {
                is String -> k
                is Symbol -> normalForm(k).toString()
                is Keyword -> normalForm(k.sym).toString()
                else -> throw IllegalArgumentException("Column name must be a string, keyword or symbol")
            }

            if (v is PositionedScalar && v.tryWriteTo(this, normalKey)) continue
            val value = if (v is PositionedScalar) v.readObject() else v
            val vector = vectorForOrNull(normalKey) ?: vectorFor(normalKey, value.toArrowType(), value == null)

            try {
                vector.writeObject(value)
            } catch (_: InvalidWriteObjectException) {
                vectorFor(normalKey, value.toArrowType(), value == null).writeObject(value)
            }
        }

        endRow()
    }

    /** Appends a compatible struct row to the existing columns. Returns false before any writes if it cannot copy. */
    fun tryCopyStructRow(src: VectorReader, idx: Int): Boolean {
        if (src.arrowType != STRUCT_TYPE || src.type.encoding != Encoding.Plain || src.isNull(idx)) return false

        val children = src.keyNames?.mapNotNull { name ->
            val child = src.vectorFor(name)
            val out = vectorForOrNull(name) ?: return false
            val target = if (child is StructVector && out is StructVector) {
                if (!child.isNull(idx) && !child.canCopyObjectValueTo(out, idx)) return false
                out
            } else out.tableCellObjectTarget(child, idx) ?: return false
            if (child.isNull(idx)) null else child to target
        } ?: return false

        for ((child, target) in children) {
            if (child is StructVector) child.rowCopier(target).copyRow(idx)
            else target.copyNonNullCell(child, idx)
        }

        endRow()
        return true
    }

    fun writeRows(vararg rows: Map<*, *>) {
        rows.forEach(::writeRow)
    }

    /**
     * Resets the row count and all vectors, leaving the buffers allocated.
     */
    fun clear() {
        vectors.forEach { it.clear() }
        rowCount = 0
    }

    override fun close() {
        vectors.closeAll()
        rowCount = 0
    }
}
