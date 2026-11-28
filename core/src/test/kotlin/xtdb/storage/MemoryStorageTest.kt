package xtdb.storage

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.api.storage.Storage
import xtdb.arrow.IntVector
import xtdb.arrow.Relation
import xtdb.cache.MemoryCache
import xtdb.util.asPath
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels.newChannel

class MemoryStorageTest : StorageTest() {
    override fun storage(): BufferPool = memoryStorage

    override fun openPartitionedStorage(partition: Int, totalPartitions: Int): BufferPool =
        Storage.inMemory().open(allocator, memoryCache, null, "xtdb", partition, totalPartitions)

    private lateinit var allocator: BufferAllocator
    private lateinit var memoryCache: MemoryCache
    private lateinit var memoryStorage: MemoryStorage

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        memoryCache = MemoryCache.Factory().open(allocator)
        memoryStorage = MemoryStorage(allocator, epoch = 0)
    }

    @AfterEach
    fun tearDown() {
        memoryStorage.close()
        memoryCache.close()
        allocator.close()
    }

    private fun openIntRelation() = Relation(allocator, listOf(IntVector.open(allocator, "value", false)), 0)

    @Test
    fun `end publishes exactly the emitted Arrow file including its footer`() {
        val key = "pages.arrow".asPath
        val expected = ByteArrayOutputStream()
        openIntRelation().use { rel ->
            rel.startUnload(newChannel(expected)).use { reference ->
                memoryStorage.openArrowWriter(key, rel).use { writer ->
                    for (page in 0..1) {
                        repeat(300) { row ->
                            rel["value"].writeInt(page * 300 + row)
                            rel.endRow()
                        }
                        reference.writePage()
                        writer.writePage()
                        rel.clear()
                    }

                    assertTrue(memoryStorage.listAllObjects().none())
                    reference.end()
                    assertEquals(expected.size().toLong(), writer.endSync())
                }
            }

            assertArrayEquals(expected.toByteArray(), memoryStorage.getByteArray(key))
            assertEquals(listOf(StoredObject(key, expected.size().toLong())), memoryStorage.listAllObjects())
            val footer = runBlocking { memoryStorage.getFooter(key) }
            assertEquals(rel.schema, footer.schema)
            assertEquals(2, footer.recordBatches.size)
            memoryStorage.getRecordBatchSync(key, 1).use { rel.load(it) }
            assertEquals(300, rel.rowCount)
            assertEquals(300, rel["value"].getInt(0))
            assertEquals(599, rel["value"].getInt(299))
        }
    }

    @Test
    fun `abandon and overwrite preserve the original batch until its reader closes`() {
        val key = "replace.arrow".asPath
        RootAllocator().use { fileAllocator ->
            MemoryStorage(fileAllocator, epoch = 0).use { storage ->
                openIntRelation().use { rel ->
                    rel["value"].writeInt(42)
                    rel.endRow()
                    val originalBytes = rel.asArrowFile
                    storage.openArrowWriter(key, rel).use { writer ->
                        writer.writePage()
                        writer.endSync()
                    }

                    storage.getRecordBatchSync(key, 0).use { oldBatch ->
                        rel.clear()
                        rel["value"].writeInt(99)
                        rel.endRow()
                        storage.openArrowWriter(key, rel).use { it.writePage() }
                        assertArrayEquals(originalBytes, storage.getByteArray(key))
                        assertEquals(originalBytes.size.toLong(), fileAllocator.allocatedMemory)

                        val replacement = rel.asArrowFile
                        storage.openArrowWriter(key, rel).use { writer ->
                            writer.writePage()
                            assertArrayEquals(originalBytes, storage.getByteArray(key))
                            writer.endSync()
                        }
                        assertArrayEquals(replacement, storage.getByteArray(key))
                        assertEquals((originalBytes.size + replacement.size).toLong(), fileAllocator.allocatedMemory)
                        storage.deleteIfExistsSync(key)
                        assertTrue(storage.listAllObjects().none())
                        assertEquals(originalBytes.size.toLong(), fileAllocator.allocatedMemory)

                        Relation.fromRecordBatch(fileAllocator, rel.schema, oldBatch).use { oldRel ->
                            assertEquals(42, oldRel["value"].getInt(0))
                        }
                    }
                    assertEquals(0L, fileAllocator.allocatedMemory)
                }
            }
        }
    }

    @Test
    fun `refusing the exact file reservation leaves the old object visible`() {
        val key = "capped.arrow".asPath
        openIntRelation().use { rel ->
            rel["value"].writeInt(42)
            rel.endRow()
            val expected = rel.asArrowFile
            RootAllocator(expected.size.toLong()).use { fileAllocator ->
                MemoryStorage(fileAllocator, epoch = 0).use { storage ->
                    storage.openArrowWriter(key, rel).use { writer ->
                        writer.writePage()
                        assertEquals(expected.size.toLong(), writer.endSync())
                    }
                    rel["value"].setInt(0, 99)
                    storage.openArrowWriter(key, rel).use { writer ->
                        writer.writePage()
                        assertThrows(OutOfMemoryException::class.java) { writer.endSync() }
                    }
                    assertArrayEquals(expected, storage.getByteArray(key))
                    assertEquals(expected.size.toLong(), fileAllocator.allocatedMemory)
                }
                assertEquals(0L, fileAllocator.allocatedMemory)
            }
        }
    }
}
