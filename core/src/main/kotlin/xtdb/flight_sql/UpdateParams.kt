package xtdb.flight_sql

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.VectorUnloader
import org.apache.arrow.vector.ipc.message.ArrowBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema

internal class UpdateParams private constructor(
    private val schema: Schema,
    private val rowCount: Int,
    private val nodes: List<ArrowFieldNode>,
    private val layout: List<ArrowBuffer>,
    private val body: ByteArray,
) : PreparedParams {
    override fun <R> read(allocator: BufferAllocator, f: (VectorSchemaRoot) -> R): R =
        VectorSchemaRoot.create(schema, allocator).use { root ->
            allocator.buffer(body.size.toLong()).use { buf ->
                buf.writeBytes(body)
                val buffers = layout.map { buf.slice(it.offset, it.size) }
                ArrowRecordBatch(rowCount, nodes, buffers).use { batch -> VectorLoader(root).load(batch) }
            }
            f(root)
        }

    companion object {
        fun fromFlightRoot(root: VectorSchemaRoot): UpdateParams? {
            if (root.schema.customMetadata.isNotEmpty()) return null
            val fields = root.plainParameterFields() ?: return null

            return VectorUnloader(root).recordBatch.use { batch ->
                val bodyLength = batch.computeBodyLength()
                if (bodyLength > Int.MAX_VALUE) return@use null

                val body = ByteArray(bodyLength.toInt())
                val layout = batch.buffersLayout
                for ((idx, buffer) in batch.buffers.withIndex()) {
                    val range = layout[idx]
                    if (range.size == 0L) continue
                    buffer.getBytes(buffer.readerIndex(), body, range.offset.toInt(), range.size.toInt())
                }
                UpdateParams(Schema(fields), batch.length, batch.nodes, layout, body)
            }
        }
    }
}
