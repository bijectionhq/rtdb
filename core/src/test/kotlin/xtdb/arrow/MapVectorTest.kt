package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.Text
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE

class MapVectorTest {
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun mapVector() {
        val m1 = mapOf(4 to "Hello", 8 to null)
        val m2 = mapOf(12 to "world")
        val m3 = mapOf(3 to "!")

        val children = linkedMapOf<String, Vector>(
            "key" to IntVector.open(allocator, "key", false),
            "value" to Utf8Vector(allocator, "value", true)
        )

        val struct = StructVector(allocator, "struct", false, children)
        val list = ListVector(allocator, "entries", false, struct)
        MapVector(list, true).use { mapVec ->
            mapVec.writeObject(m1)
            mapVec.writeObject(m2)
            mapVec.writeObject(m3)

            assertEquals(listOf(m1, m2, m3), mapVec.asList)
        }
    }

    @Test
    fun `value writes copy unique map entries and preserve map decoding`() {
        for (keysSorted in listOf(false, true)) {
            val type = VectorType.mapTypeOf(VectorType.I32, VectorType.maybe(VectorType.UTF8), keysSorted)
            Vector.open(allocator, "src", type).use { src ->
                src.writeObject(linkedMapOf(1 to "one", 2 to null))
                val repeated = listOf(3 to "first", 4 to "four", 3 to "last")
                for ((key, value) in if (keysSorted) repeated.sortedBy { it.first } else repeated) {
                    src.mapKeys.writeInt(key)
                    src.mapValues.writeObject(value)
                    src.listElements.endStruct()
                }
                src.endList()
                src.mapKeys.writeInt(5)
                src.mapValues.writeBytes(byteArrayOf(0xff.toByte()))
                src.listElements.endStruct()
                src.endList()

                Vector.open(allocator, "dst", type).use { dst ->
                    val reader = src.valueReader()
                    assertTrue(reader.next().copyIdentityTo(dst))
                    dst.writeValue(reader.next())
                    dst.writeValue(reader.next())

                    assertEquals(ArrowType.Map(keysSorted), dst.arrowType)
                    assertEquals(listOf(1, 2, 3, 4, 5), dst.mapKeys.asList)
                    assertEquals(listOf("one", null, "last", "four", "\uFFFD"), dst.mapValues.asList)
                    assertEquals(ByteBuffer.wrap("\uFFFD".toByteArray()), dst.mapValues.getBytes(4))
                    assertEquals(listOf(2, 2, 1), (0 until dst.valueCount).map(dst::getListCount))
                }
            }
        }
    }

    @Test
    fun `positioned maps copy unique unicode and boolean keys in entry order`() {
        val cases = listOf(
            VectorType.UTF8 to listOf("é", "😀"),
            VectorType.BOOL to listOf(false, true)
        )
        for ((keyType, keys) in cases) {
            val type = VectorType.mapTypeOf(keyType, VectorType.UTF8, false)
            Vector.open(allocator, "src", type).use { src ->
                src.writeObject(linkedMapOf(keys[0] to "début", keys[1] to "🎈"))
                for ((key, value) in listOf(keys[0] to "old", keys[1] to "other", keys[0] to "new")) {
                    src.mapKeys.writeObject(key)
                    src.mapValues.writeObject(value)
                    src.listElements.endStruct()
                }
                src.endList()

                Vector.open(allocator, "dst", type).use { dst ->
                    val rows = src.valueReader()
                    assertTrue(rows.next().copyIdentityTo(dst))
                    assertFalse(rows.next().copyIdentityTo(dst))
                    dst.writeValue(rows)
                    assertEquals(keys + keys, dst.mapKeys.asList)
                    assertEquals(listOf("début", "🎈", "new", "other"), dst.mapValues.asList)
                }
            }
        }
    }

    @Test
    fun `positioned binary map keys keep distinct decoded array identities`() {
        val type = VectorType.mapTypeOf(VectorType.VAR_BINARY, VectorType.UTF8, true)
        Vector.open(allocator, "src", type).use { src ->
            src.writeObject(linkedMapOf(byteArrayOf(1) to "first", byteArrayOf(1) to "second", byteArrayOf(2) to "third"))
            assertEquals(3, (src.getObject(0) as Map<*, *>).size)

            Vector.open(allocator, "dst", type).use { dst ->
                assertTrue(src.valueReader().next().copyIdentityTo(dst))
                assertEquals(ArrowType.Map(true), dst.arrowType)
                assertEquals(listOf(1, 1, 2), dst.mapKeys.asList.map { (it as ByteArray).single().toInt() })
                assertEquals(listOf("first", "second", "third"), dst.mapValues.asList)
            }
        }
    }

    @Test
    fun `malformed utf8 keys collapse with their java replacement at the first position`() {
        val type = VectorType.mapTypeOf(VectorType.UTF8, VectorType.UTF8, false)
        Vector.open(allocator, "src", type).use { src ->
            val entries = listOf(
                "\uFFFD".toByteArray() to "first",
                "other".toByteArray() to "middle",
                byteArrayOf(0xff.toByte()) to "last"
            )
            for ((key, value) in entries) {
                src.mapKeys.writeBytes(key)
                src.mapValues.writeObject(value)
                src.listElements.endStruct()
            }
            src.endList()

            Vector.open(allocator, "dst", type).use { dst ->
                val row = src.valueReader().next()
                assertFalse(row.copyIdentityTo(dst))
                dst.writeValue(row)
                assertEquals(listOf("\uFFFD", "other"), dst.mapKeys.asList)
                assertEquals(listOf("last", "middle"), dst.mapValues.asList)
                assertEquals(ByteBuffer.wrap("\uFFFD".toByteArray()), dst.mapKeys.getBytes(0))
            }
        }
    }

    @Test
    fun roundTripsThroughFile() {
        val m1 = mapOf(4 to "Hello", 8 to null)
        val m2 = mapOf(12 to "world")
        val m3 = mapOf(3 to "!")

        val children = linkedMapOf<String, Vector>(
            "key" to IntVector.open(allocator, "key", false),
            "value" to Utf8Vector(allocator, "value", true)
        )

        val struct = StructVector(allocator, "entries", false, children)
        val list = ListVector(allocator, "map", false, struct)
        val map = MapVector(list, true)

        val buf = ByteArrayOutputStream()

        Relation(allocator, listOf(map), 0).use { rel ->
            rel.startUnload(Channels.newChannel(buf)).use { unloader ->
                map.writeObject(m1)
                rel.endRow()

                map.writeObject(m2)
                rel.endRow()

                unloader.writePage()
                rel.clear()

                map.writeObject(m3)
                rel.endRow()

                unloader.writePage()
                unloader.end()
            }
        }

        Relation.loader(allocator, buf.toByteArray().asChannel).use { loader ->
            Relation(allocator, loader.schema).use { rel ->
                val mapVec = rel["map"]

                assertEquals(2, loader.pageCount)

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(listOf(m1, m2), mapVec.asList)

                loader.loadPage(1, rel)

                assertEquals(1, rel.rowCount)
                assertEquals(listOf(m3), mapVec.asList)

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(listOf(m1, m2), mapVec.asList)
            }
        }

        // reads back in with original Type
        val rdr = ArrowFileReader(SeekableReadChannel(buf.toByteArray().asChannel), allocator)
            .also { it.initialize() }

        rdr.vectorSchemaRoot.use { vsr ->
            assertEquals(
                Schema(
                    listOf(
                        Field(
                            "map", FieldType.notNullable(ArrowType.Map(true)),
                            listOf(
                                Field(
                                    "entries", FieldType.notNullable(STRUCT_TYPE),
                                    listOf(
                                        Field("key", FieldType.notNullable(MinorType.INT.type), emptyList()),
                                        Field("value", FieldType.nullable(MinorType.VARCHAR.type), emptyList()),
                                    )
                                )
                            )
                        )
                    )
                ),
                vsr.schema
            )

            val mapVec = vsr.getVector("map")

            assertTrue(rdr.loadNextBatch())
            assertEquals(2, vsr.rowCount)
            assertEquals(listOf(mapOf("key" to 4, "value" to Text("Hello")), mapOf("key" to 8)), mapVec.getObject(0))
            assertEquals(listOf(mapOf("key" to 12, "value" to Text("world"))), mapVec.getObject(1))

            assertTrue(rdr.loadNextBatch())
            assertEquals(1, vsr.rowCount)
            assertEquals(listOf(mapOf("key" to 3, "value" to Text("!"))), mapVec.getObject(0))

            assertFalse(rdr.loadNextBatch())
        }


    }

}
