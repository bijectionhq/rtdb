package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import xtdb.arrow.RowMask
import xtdb.arrow.Vector
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType
import java.nio.ByteBuffer

private fun compareUnsigned(left: ByteBuffer, right: ByteBuffer): Int {
    val mismatch = left.mismatch(right)
    return if (mismatch < 0 || mismatch == minOf(left.remaining(), right.remaining()))
        left.remaining().compareTo(right.remaining())
    else
        java.lang.Byte.compareUnsigned(left.get(left.position() + mismatch), right.get(right.position() + mismatch))
}

class VarWidthMinMax(
    private val fromName: FieldName,
    override val colName: FieldName,
    override val type: VectorType,
    private val hasZeroRow: Boolean,
    private val isMin: Boolean,
) : AggregateSpec.Factory {

    override fun build(al: BufferAllocator, args: RelationReader) = object : AggregateSpec {
        private val winners = ArrayList<ByteBuffer?>()

        override fun aggregate(inRel: RelationReader, groupMapping: GroupMapping, mask: RowMask) {
            val inVec = inRel.vectorForOrNull(fromName) ?: return

            repeat(inRel.rowCount) { idx ->
                val groupIdx = groupMapping.getInt(idx)

                while (winners.size <= groupIdx)
                    winners.add(null)

                if (mask.isSet(idx) && !inVec.isNull(idx)) {
                    val candidate = inVec.getBytes(idx)
                    val current = winners[groupIdx]

                    if (current == null || compareUnsigned(candidate, current).let { if (isMin) it < 0 else it > 0 }) {
                        winners[groupIdx] = ByteBuffer.wrap(ByteArray(candidate.remaining()).also { candidate.duplicate().get(it) })
                    }
                }
            }
        }

        override fun openFinishedVector(): Vector {
            val outVec = al.openVector(colName, type)

            for (winner in winners) {
                if (winner == null) outVec.writeNull()
                else outVec.writeBytes(winner)
            }

            if (hasZeroRow && winners.isEmpty())
                outVec.writeNull()

            return outVec
        }

        override fun close() {}
    }
}
