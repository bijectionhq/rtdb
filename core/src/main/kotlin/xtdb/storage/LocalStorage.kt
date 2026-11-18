package xtdb.storage

import com.github.benmanes.caffeine.cache.AsyncCache
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import xtdb.ArrowWriter
import xtdb.IEvictBufferTest
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage.arrowFooterCache
import xtdb.arrow.ArrowUtil.arrowBufToRecordBatch
import xtdb.arrow.ArrowUtil.readArrowFooter
import xtdb.arrow.ArrowUtil.toByteArray
import xtdb.arrow.Relation
import xtdb.cache.MemoryCache
import xtdb.trie.FileSize
import xtdb.util.*
import java.io.Closeable
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitResult.CONTINUE
import java.nio.file.FileVisitResult.SKIP_SUBTREE
import java.nio.file.Files
import java.nio.file.Files.newByteChannel
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.*

internal class LocalStorage(
    allocator: BufferAllocator,
    private val memoryCache: MemoryCache.ForDatabase,
    meterRegistry: MeterRegistry? = null,
    override val epoch: StorageEpoch,
    val rootPath: Path,
) : BufferPool, IEvictBufferTest, Closeable {

    private val allocator = allocator.openChildAllocator("buffer-pool")
    private val uploadDirectory = rootPath.resolve(".tmp")

    private val arrowFooterCache: AsyncCache<Path, ArrowFooter> = arrowFooterCache()
    private val recordBatchRequests: Counter? = meterRegistry?.counter("record-batch-requests")
    private val memCacheMisses: Counter? = meterRegistry?.counter("memory-cache-misses")

    companion object {
        private fun objectMissingException(path: Path) = IllegalStateException("Object $path doesn't exist.")

        private fun Path.orThrowIfMissing(key: Path) = takeIf { it.exists() } ?: throw objectMissingException(key)
    }

    private fun createTempUploadFile(): Path =
        createTempFile(uploadDirectory.createDirectories(), "upload", ".arrow")

    override fun getByteArray(key: Path): ByteArray =
        runBlocking {
            memoryCache.get(key) { k ->
                memCacheMisses?.increment()
                Pair(this@LocalStorage.rootPath.resolve(k).orThrowIfMissing(key), null)
            }.use { it.toByteArray() }
        }

    override suspend fun getFooter(key: Path): ArrowFooter =
        arrowFooterCache.get(key) { _, _ ->
            val path = rootPath.resolve(key).orThrowIfMissing(key)

            completedFuture(path.openReadableChannel().readArrowFooter())
        }.asDeferred().await()

    override suspend fun getRecordBatch(key: Path, idx: Int): ArrowRecordBatch {
        recordBatchRequests?.increment()
        val path = rootPath.resolve(key).orThrowIfMissing(key)

        val footer = arrowFooterCache.get(key) { _, _ ->
            completedFuture(path.openReadableChannel().readArrowFooter())
        }.asDeferred().await()

        val arrowBlock = footer.recordBatches.getOrNull(idx)
            ?: throw IndexOutOfBoundsException("Record batch index out of bounds of arrow file")

        return memoryCache.get(
            key,
            MemoryCache.Slice(arrowBlock.offset, arrowBlock.metadataLength + arrowBlock.bodyLength)
        ) { k ->
            memCacheMisses?.increment()
            Pair(rootPath.resolve(k).orThrowIfMissing(k), null)
        }.use { arrowBuf ->
            arrowBuf.arrowBufToRecordBatch(
                0,
                arrowBlock.metadataLength,
                arrowBlock.bodyLength,
                "Failed opening record batch '$path' at block-idx $idx"
            )
        }
    }

    private fun ByteBuffer.writeToPath(path: Path) {
        newByteChannel(path, WRITE, TRUNCATE_EXISTING, CREATE).use { channel ->
            while (hasRemaining()) channel.write(this)
        }
    }

    override suspend fun putObject(key: Path, buffer: ByteBuffer) {
        try {
            val tmpPath = createTempUploadFile()
            buffer.writeToPath(tmpPath)

            val filePath = rootPath.resolve(key).also { it.createParentDirectories() }
            tmpPath.moveTo(filePath, StandardCopyOption.ATOMIC_MOVE)
        } catch (_: ClosedByInterruptException) {
            throw InterruptedException()
        }
    }

    private fun Path.listAll(): List<StoredObject> {
        val staging = uploadDirectory.normalize()
        if (normalize().startsWith(staging)) return emptyList()

        val objects = mutableListOf<StoredObject>()
        Files.walkFileTree(this, object : SimpleFileVisitor<Path>() {
            override fun preVisitDirectory(dir: Path, attrs: BasicFileAttributes) =
                if (dir.normalize() == staging) SKIP_SUBTREE else CONTINUE

            override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                if (file.normalize() != staging) {
                    val size = if (attrs.isSymbolicLink) file.fileSize() else attrs.size()
                    objects.add(StoredObject(rootPath.relativize(file), size))
                }
                return CONTINUE
            }

            override fun visitFileFailed(file: Path, error: IOException): FileVisitResult {
                if (error is NoSuchFileException) return CONTINUE
                throw error
            }
        })
        return objects.sortedBy { it.key }
    }

    override fun listAllObjects(): Iterable<StoredObject> = rootPath.listAll()
    override fun listAllObjects(dir: Path) = rootPath.resolve(dir).listAll()

    override suspend fun copyObject(src: Path, dest: Path) {
        Files.copy(
            rootPath.resolve(src).normalize(),
            rootPath.resolve(dest).normalize().also { it.createParentDirectories() }
        )
    }

    override suspend fun deleteIfExists(key: Path) {
        rootPath.resolve(key).deleteIfExists()
    }

    override fun openArrowWriter(key: Path, rel: Relation): ArrowWriter {
        val tmpPath = createTempUploadFile()
        return newByteChannel(tmpPath, WRITE, TRUNCATE_EXISTING, CREATE).closeOnCatch { fileChannel ->
            rel.startUnload(fileChannel).closeOnCatch { unloader ->
                object : ArrowWriter {
                    override fun writePage() {
                        try {
                            unloader.writePage()
                        } catch (_: ClosedByInterruptException) {
                            throw InterruptedException()
                        }
                    }

                    override suspend fun end(): FileSize {
                        unloader.end()
                        fileChannel.close()

                        val filePath = rootPath.resolve(key).also { it.createParentDirectories() }
                        tmpPath.moveTo(filePath, StandardCopyOption.ATOMIC_MOVE)
                        return filePath.fileSize()
                    }

                    override fun close() {
                        unloader.close()
                        if (fileChannel.isOpen) fileChannel.close()
                        tmpPath.deleteIfExists()
                    }
                }
            }
        }
    }

    override fun close() {
        allocator.close()
    }
}
