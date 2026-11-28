package xtdb.arrow

import xtdb.InternalApi
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING

/** A non-null scalar cell borrowed from an open argument relation. */
@InternalApi
class PositionedScalar private constructor(private val reader: Vector, private val index: Int) {
    fun readObject(): Any? = reader.getObject(index, SNAKE_CASE_STRING)

    fun tryWriteTo(relation: RelationWriter, name: String): Boolean {
        val type = reader.arrowType
        val target = when (val existing = relation.vectorForOrNull(name)) {
            null, is NullVector -> relation.vectorFor(name, type, false)
            is DenseUnionVector -> existing.vectorFor(type, false)
            else -> existing.takeIf { it.arrowType == type } ?: return false
        }
        target.writeValue0(reader.valueReader().also { it.pos = index })
        return true
    }

    companion object {
        @JvmStatic
        fun read(reader: VectorReader, index: Int): Any? {
            val source = VectorReader.unwrapRenamed(reader)
            if (source !is MonoVector) return reader.getObject(index, SNAKE_CASE_STRING)

            if (source.isNull(index)) return null
            // Even unreferenced cells are read before the static-op visitor can decline expansion.
            when (source) {
                is BitVector -> source.getBoolean(index)
                is ByteVector -> source.getByte(index)
                is ShortVector -> source.getShort(index)
                is IntVector -> source.getInt(index)
                is DateDayVector -> source.getInt(index)
                is LongVector -> source.getLong(index)
                is FloatVector -> source.getFloat(index)
                is DoubleVector -> source.getDouble(index)
                is Utf8Vector -> {
                    if (!source.getBytes(index).isWellFormedUtf8())
                        return reader.getObject(index, SNAKE_CASE_STRING)
                }
                is VarBinaryVector -> source.getBytes(index)
                is UuidVector -> {
                    if (!source.hasCanonicalWidth) return reader.getObject(index, SNAKE_CASE_STRING)
                    source.getBytes(index)
                }
                else -> return reader.getObject(index, SNAKE_CASE_STRING)
            }
            return PositionedScalar(source, index)
        }
    }
}
