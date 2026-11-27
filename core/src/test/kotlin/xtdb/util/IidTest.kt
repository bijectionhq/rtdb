@file:OptIn(xtdb.InternalApi::class)

package xtdb.util

import clojure.lang.Keyword
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.roaringbitmap.buffer.MutableRoaringBitmap
import xtdb.api.error.Incorrect
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.KeywordVector
import xtdb.arrow.Utf8Vector
import xtdb.arrow.Vector
import xtdb.arrow.VectorType
import xtdb.bloom.BLOOM_K
import xtdb.bloom.bloomHashesInto
import xtdb.bloom.bloomHashes
import java.nio.ByteBuffer

class IidTest {

    @Test
    fun `keyword IID retains canonical spelling and Java replacement for malformed UTF-8`() {
        RootAllocator().use { allocator ->
            KeywordVector(Utf8Vector(allocator, "_id", true)).use { ids ->
                for (spelling in listOf("plain", "ns/clé", "/", "/leading", "trailing/", "a/b/c")) {
                    val idx = ids.valueCount
                    ids.writeBytes(spelling.toByteArray())
                    val expected = Keyword.intern(spelling).asIid
                    assertArrayEquals(expected, iidFromId(ids, idx))
                    assertArrayEquals(expected, iidFromIdOrNull(ids, idx))
                }
                val malformed = byteArrayOf(0xed.toByte(), 0xa0.toByte(), 0x80.toByte())
                val idx = ids.valueCount
                ids.writeBytes(malformed)
                val expected = Keyword.intern(malformed.toString(Charsets.UTF_8)).asIid
                assertArrayEquals(expected, iidFromId(ids, idx))
                assertArrayEquals(expected, iidFromIdOrNull(ids, idx))
                ids.writeNull()
                assertNull(iidFromIdOrNull(ids, idx + 1))
                assertThrows<Incorrect> { iidFromId(ids, idx + 1) }
            }
        }
    }

    @Test
    fun `keyword IID follows the selected canonical union leg`() {
        RootAllocator().use { allocator ->
            val type = VectorType.fromLegs(VectorType.I64, VectorType.KEYWORD)
            Vector.open(allocator, "_id", type).use { ids ->
                val keyword = Keyword.intern("ns/東京")
                ids.writeObject(4L)
                ids.writeObject(keyword)
                ids.select(intArrayOf(1, 0)).use { selected ->
                    assertArrayEquals(keyword.asIid, iidFromId(selected, 0))
                    assertArrayEquals(4L.asIid, iidFromId(selected, 1))
                }
            }
        }
    }

    @Test
    fun `physical IID read owns the selected key after its Arrow vector closes`() {
        val expected = ByteArray(16) { (it * 17).toByte() }
        val key = RootAllocator().use { allocator ->
            FixedSizeBinaryVector(allocator, "_iid", true, 16).use { ids ->
                ids.writeNull()
                ids.writeBytes(ByteBuffer.wrap(expected))
                assertNull(ids.getObject(0))
                ids.getObject(1) as ByteArray
            }
        }
        assertArrayEquals(expected, key)
    }

    @Test
    fun `join Bloom inserts the same positions for physical IID rows and nulls`() {
        RootAllocator().use { allocator ->
            FixedSizeBinaryVector(allocator, "_iid", true, 16).use { ids ->
                ids.writeBytes(ByteArray(16) { it.toByte() })
                ids.writeNull()
                ids.writeBytes(ByteArray(16) { (255 - it).toByte() })
                val expected = MutableRoaringBitmap()
                val actual = MutableRoaringBitmap()
                val positions = IntArray(BLOOM_K)
                for (idx in 0 until ids.valueCount) {
                    val previous = bloomHashes(ids, idx)
                    expected.add(*previous)
                    bloomHashesInto(ids, idx, positions)
                    assertArrayEquals(previous, positions)
                    actual.add(*positions)
                }
                assertEquals(expected, actual)
            }
        }
    }
}
