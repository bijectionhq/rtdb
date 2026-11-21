package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.IntVector
import xtdb.trie.proto.branch
import xtdb.trie.proto.hashTrie
import xtdb.trie.proto.hashTrieNode
import xtdb.trie.proto.leaf
import java.util.Arrays
import kotlin.random.Random

class MemoryHashTrieTest {
    private lateinit var allocator: BufferAllocator
    private lateinit var iidReader: IntVector

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        iidReader = IntVector.open(allocator, "_iid", true)
    }

    @AfterEach
    fun tearDown() {
        iidReader.close()
        allocator.close()
    }

    private fun emptyTrie(logLimit: Int = 8, pageLimit: Int = 32): MemoryHashTrie =
        MemoryHashTrie.builder(iidReader)
            .setLogLimit(logLimit)
            .setPageLimit(pageLimit)
            .build()

    private fun writeRandomIids(count: Int, seed: Long = 42L) {
        val r = Random(seed)
        for (i in 0 until count) iidReader.writeInt(r.nextInt())
    }

    /**
     * Indices reachable from any leaf, after compaction. Tree shape may vary
     * legitimately by insertion order; the contained-index multiset is the
     * invariant that addAll/addRange must preserve.
     */
    private fun MemoryHashTrie.indexSet(): Set<Int> =
        compactLogs().leaves.flatMap { it.data.toList() }.toSet()

    @Test
    fun `addRange covers same indices as folded plus, within log limit`() {
        val n = 6
        writeRandomIids(n)
        val byRange = emptyTrie().addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie()) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `addRange covers same indices as folded plus, across log compactions`() {
        // logLimit = 8, pageLimit = 32: 20 entries forces at least one compaction
        // but stays under the page split threshold.
        val n = 20
        writeRandomIids(n)
        val byRange = emptyTrie().addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie()) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `addRange covers same indices as folded plus, across page splits`() {
        val n = 200
        writeRandomIids(n)
        val byRange = emptyTrie().addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie()) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `addRange in chunks covers same indices as one big addRange`() {
        val n = 150
        writeRandomIids(n)
        val byChunks = emptyTrie()
            .addRange(0, 7)
            .addRange(7, 50)
            .addRange(57, n - 57)
        val byOne = emptyTrie().addRange(0, n)
        assertEquals(byOne.indexSet(), byChunks.indexSet())
        assertEquals((0 until n).toSet(), byOne.indexSet())
    }

    @Test
    fun `addRange exactly at log limit triggers single compaction`() {
        val n = 8
        writeRandomIids(n)
        val byRange = emptyTrie(logLimit = 8, pageLimit = 32).addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie(logLimit = 8, pageLimit = 32)) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `single inserts from the same trie preserve both versions`() {
        writeRandomIids(3)
        val base = emptyTrie() + 0
        val first = base + 1
        val second = base + 2

        assertEquals(setOf(0), base.indexSet())
        assertEquals(setOf(0, 1), first.indexSet())
        assertEquals(setOf(0, 2), second.indexSet())
    }

    @Test
    fun `batches below the log limit preserve sibling versions`() {
        writeRandomIids(5)
        val base = emptyTrie() + 0
        val first = base.addAll(intArrayOf(1, 2))
        val second = base.addAll(intArrayOf(3, 4))

        assertEquals(setOf(0), base.indexSet())
        assertEquals(setOf(0, 1, 2), first.indexSet())
        assertEquals(setOf(0, 3, 4), second.indexSet())
    }

    @Test
    fun `a range filling the log preserves an earlier sibling version`() {
        writeRandomIids(4)
        val base = emptyTrie(logLimit = 3) + 0
        val first = base + 1
        val second = base.addRange(2, 2)

        assertEquals(setOf(0), base.indexSet())
        assertEquals(setOf(0, 1), first.indexSet())
        assertEquals(setOf(0, 2, 3), second.indexSet())
    }

    @Test
    fun `leaf order matches unsigned IID bytes and descending row indices`() {
        val iids = (0 until 16).flatMap { position ->
            (0..255).map { byte -> ByteArray(16).also { it[position] = byte.toByte() } }
        }.shuffled(Random(76))

        FixedSizeBinaryVector(allocator, "_iid", false, 16).use { vector ->
            for (iid in iids) vector.writeBytes(iid)

            var trie = MemoryHashTrie.builder(vector).setPageLimit(iids.size).build()
            for (start in iids.indices step 53) {
                trie = trie.addRange(start, minOf(53, iids.size - start))
            }

            val expected = iids.indices.sortedWith { left, right ->
                val order = Arrays.compareUnsigned(iids[left], iids[right])
                if (order == 0) right.compareTo(left) else order
            }
            assertEquals(expected, trie.leaves.single().mergeSort(trie).toList())
        }
    }

    @Test
    fun `protobuf restores root and bucket paths for lookup and later inserts`() {
        FixedSizeBinaryVector(allocator, "_iid", false, 16).use { iids ->
            val root = byteArrayOf(2)
            val first = byteArrayOf(2, 1)
            val second = byteArrayOf(2, 3)
            val later = byteArrayOf(2, 2)
            for (path in listOf(first, second, first, later)) {
                iids.writeBytes(Bucketer.DEFAULT.startIid(path))
            }

            val original = MemoryHashTrie.builder(iids)
                .setRootPath(root)
                .setPageLimit(2)
                .setLogLimit(2)
                .build()
                .addRange(0, 3)

            val restored = MemoryHashTrie.fromProto(original.asProto, iids)
            assertEquals(root.toList(), restored.rootNode.path.toList())
            assertEquals(listOf(first.toList(), second.toList()), restored.leaves.map { it.path.toList() })
            assertEquals(listOf(listOf(2, 0), listOf(1)), restored.leaves.map { it.data.toList() })
            val iid = Bucketer.DEFAULT.startIid(first)
            val candidates = restored.leaves.filter { Bucketer.DEFAULT.compareToPath(iid, it.path) == 0 }
            assertEquals(listOf(2, 0), candidates.flatMap { it.data.toList() })

            val extended = restored.addRange(3, 1).compactLogs()
            assertEquals(
                listOf(first.toList(), later.toList(), second.toList()),
                extended.leaves.map { it.path.toList() }
            )
            assertEquals(listOf(listOf(2, 0), listOf(3), listOf(1)), extended.leaves.map { it.data.toList() })
        }
    }

    @Test
    fun `protobuf derives paths from bucket positions in legacy rootless messages`() {
        val encoded = hashTrie {
            logLimit = 8
            pageLimit = 32
            rootNode = hashTrieNode {
                branch = branch {
                    children.addAll(listOf(
                        hashTrieNode {},
                        hashTrieNode { leaf = leaf { data.add(0) } },
                        hashTrieNode {},
                        hashTrieNode {},
                    ))
                }
            }
        }.toByteArray()

        val restored = MemoryHashTrie.fromProto(encoded, iidReader)
        assertEquals(0, restored.rootNode.path.size)
        assertEquals(byteArrayOf(1).toList(), restored.leaves.single().path.toList())
        assertEquals(listOf(0), restored.leaves.single().data.toList())
    }

    @Test
    fun `addRange with zero count is identity`() {
        val trie = emptyTrie()
        assertSame(trie, trie.addRange(0, 0))
    }

    @Test
    fun `addAll empty is identity`() {
        val trie = emptyTrie()
        assertSame(trie, trie.addAll(IntArray(0)))
    }
}
