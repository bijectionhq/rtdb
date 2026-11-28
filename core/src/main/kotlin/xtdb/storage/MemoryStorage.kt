package xtdb.storage

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.ArrowWriter
import xtdb.IEvictBufferTest
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.arrow.ArrowUtil.openArrowBufView
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toArrowRecordBatchView
import xtdb.arrow.ArrowUtil.toByteArray
import xtdb.arrow.Relation
import xtdb.api.error.Unsupported
import xtdb.trie.FileSize
import xtdb.util.closeOnCatch
import xtdb.util.openChildAllocator
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels.newChannel
import java.nio.file.Path
import java.util.*

private class ArrowFileOutput : ByteArrayOutputStream() {
    fun asByteBuffer(): ByteBuffer = ByteBuffer.wrap(buf, 0, count)
}

internal class MemoryStorage(
    allocator: BufferAllocator,
    override val epoch: StorageEpoch
) : BufferPool, IEvictBufferTest {

    private val allocator = allocator.openChildAllocator("buffer-pool")

    private val memoryStore: NavigableMap<Path, ArrowBuf> = TreeMap()

    private fun openObject(key: Path): ArrowBuf = synchronized(memoryStore) {
        val buf = memoryStore[key] ?: throw IllegalStateException("Object $key doesn't exist.")
        buf.referenceManager.retain()
        buf
    }

    override fun getByteArray(key: Path): ByteArray =
        openObject(key).use { it.toByteArray() }

    override suspend fun getFooter(key: Path): ArrowFooter =
        openObject(key).use { it.readArrowFooter() }

    override suspend fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch =
        try {
            openObject(key).use { arrowBuf ->
                val arrowBlock = arrowBuf.readArrowFooter().recordBatches.getOrNull(idx)
                    ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

                arrowBuf.toArrowRecordBatchView(arrowBlock)
            }
        } catch (e: Exception) {
            throw IllegalStateException("Failed opening record batch '$key'", e)
        }

    override suspend fun putObject(key: Path, buffer: ByteBuffer) {
        synchronized(memoryStore) {
            memoryStore.put(key, buffer.openArrowBufView(allocator))?.close()
        }
    }

    override fun listAllObjects() =
        synchronized(memoryStore) {
            memoryStore.entries.map { StoredObject(it.key, it.value.capacity()) }
        }

    override fun listAllObjects(dir: Path) =
        synchronized(memoryStore) {
            memoryStore.tailMap(dir).entries
                .takeWhile { it.key.startsWith(dir) }
                .map { StoredObject(it.key, it.value.capacity()) }
        }

    override suspend fun copyObject(src: Path, dest: Path) =
        throw Unsupported("copyObject unsupported on MemoryStorage")

    private fun deleteAllObjects() {
        synchronized(memoryStore) {
            memoryStore.values.forEach { it.close() }
            memoryStore.clear()
        }
    }

    override suspend fun deleteIfExists(key: Path): Unit =
        synchronized(memoryStore) {
            memoryStore.remove(key)?.also { it.close() }
        }

    override fun openArrowWriter(key: Path, rel: Relation): ArrowWriter {
        val output = ArrowFileOutput()
        return newChannel(output).closeOnCatch { writeChannel ->
            rel.startUnload(writeChannel).closeOnCatch { unloader ->
                object : ArrowWriter {
                    override fun writePage() {
                        unloader.writePage()
                    }

                    override suspend fun end(): FileSize {
                        val fileSize = unloader.end()
                        writeChannel.close()
                        putObject(key, output.asByteBuffer())
                        return fileSize
                    }

                    override fun close() {
                        unloader.close()
                        if (writeChannel.isOpen) {
                            writeChannel.close()
                        }
                    }
                }
            }
        }
    }

    override fun close() {
        deleteAllObjects()
        allocator.close()
    }
}
