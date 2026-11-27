@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import clojure.lang.Keyword
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase.Companion.openBase
import xtdb.TestPartition
import xtdb.api.TransactionKey
import xtdb.api.error.Anomaly
import xtdb.api.error.Anomaly.Companion.wrapAnomaly
import xtdb.api.error.Fault
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.arrow.IID_TYPE
import xtdb.arrow.LIST_TYPE
import xtdb.arrow.Relation
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.UNION_TYPE
import xtdb.arrow.UTF8_TYPE
import xtdb.arrow.VAR_BINARY_TYPE
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.schema
import xtdb.storage.MemoryStorage
import java.time.Instant
import java.time.ZoneOffset

class SourceLogTxIndexerTest {

    private val txKey = TransactionKey(0, Instant.EPOCH)

    private fun argStream(vararg values: Long): ByteArray = RootAllocator().use { allocator ->
        Relation(allocator, schema("?_0" ofType I64)).use { args ->
            for (value in values) {
                args["?_0"].writeLong(value)
                args.endRow()
            }
            args.toArrowStream().toByteArray()
        }
    }

    private fun replay(
        argType: ArrowType,
        vararg operations: Pair<String, Any?>,
        leadingErase: Boolean = false,
    ): TxResult =
        openBase(openMeterRegistry = false).use { base ->
            val allocator = base.allocator
            MemoryStorage(allocator, epoch = 0).use { storage ->
                TestPartition(allocator, storage).use { partition ->
                    val indexer = SourceLogTxIndexer(allocator, base, partition.state, "xtdb", CrashLogger(allocator, storage, "test"))
                    val opts = SourceLogTxIndexer.TxOpts(txKey, Instant.EPOCH, 0, ZoneOffset.UTC, null)

                    Relation(allocator).use { source ->
                        val ops = source.vectorFor("tx-ops", UNION_TYPE, false)
                        if (leadingErase) {
                            val erase = ops.vectorFor("erase-docs", STRUCT_TYPE, false)
                            erase.vectorFor("table", UTF8_TYPE, false).writeObject("public/docs")
                            val iids = erase.vectorFor("iids", LIST_TYPE, false)
                            iids.getListElements(IID_TYPE, false)
                            iids.endList()
                            erase.endStruct()
                            source.endRow()
                        }

                        val sql = ops.vectorFor("sql", STRUCT_TYPE, false)
                        val queries = sql.vectorFor("query", UTF8_TYPE, false)
                        val args = sql.vectorFor("args", argType, true)
                        for ((query, arg) in operations) {
                            queries.writeObject(query)
                            args.writeObject(arg)
                            sql.endStruct()
                            source.endRow()
                        }

                        OpenTx(allocator, base, partition.storage, partition.state, "xtdb", txKey, null).use { tx ->
                            indexer.ForTx(ops, opts).indexTx(tx)
                        }
                    }
                }
            }
        }

    private fun replayError(
        argType: ArrowType,
        vararg operations: Pair<String, Any?>,
        leadingErase: Boolean = false,
    ): Anomaly = try {
        val outcome = assertInstanceOf(TxResult.Aborted::class.java, replay(argType, *operations, leadingErase = leadingErase))
        assertInstanceOf(Anomaly::class.java, outcome.error)
    } catch (fault: Fault) {
        fault
    }

    private fun Anomaly.context(key: String) = getData().valAt(Keyword.intern(key))

    @Test
    fun `replay preserves the reached argument row and SQL operation context`() {
        val sql = "ASSERT ? > 0"
        val error = replayError(VAR_BINARY_TYPE, sql to argStream(5), sql to argStream(7, 0, 9), leadingErase = true)
        assertEquals(Keyword.intern("xtdb/assert-failed"), error.context("xtdb.error/code"))
        assertEquals(sql, error.context("sql"))
        assertEquals(txKey, error.context("tx-key"))
        assertEquals(2, error.context("tx-op-idx"))
        assertEquals(1, error.context("arg-idx"))
    }

    @Test
    fun `replay distinguishes absent arguments from an IPC stream with no argument rows`() {
        val sql = "ASSERT 1 = 2"
        val error = replayError(VAR_BINARY_TYPE, sql to null)
        assertEquals(Keyword.intern("xtdb/assert-failed"), error.context("xtdb.error/code"))
        assertEquals(0, error.context("tx-op-idx"))
        assertNull(error.context("arg-idx"))

        assertInstanceOf(TxResult.Committed::class.java, replay(VAR_BINARY_TYPE, sql to argStream()))
    }

    @Test
    fun `empty and malformed binary cells keep the existing IPC error and SQL context`() {
        val sql = "ASSERT 1 = 1"
        for (bytes in listOf(byteArrayOf(), byteArrayOf(-1, -1, -1, -1))) {
            val expected = RootAllocator().use { allocator ->
                assertThrows<Anomaly> { wrapAnomaly { Relation.streamLoader(allocator, bytes).use {} } }
            }

            val error = replayError(VAR_BINARY_TYPE, sql to bytes)
            assertEquals(expected.javaClass, error.javaClass)
            assertEquals(expected.message, error.message)
            assertEquals(expected.context("xtdb.error/code"), error.context("xtdb.error/code"))
            assertEquals(sql, error.context("sql"))
            assertEquals(txKey, error.context("tx-key"))
            assertEquals(0, error.context("tx-op-idx"))
            assertNull(error.context("arg-idx"))
        }
    }

    @Test
    fun `other layouts retain the logical byte-array read`() {
        val args = argStream(5)
        val sql = "ASSERT ? > 0"
        assertInstanceOf(TxResult.Committed::class.java, replay(ArrowType.FixedSizeBinary(args.size), sql to args))

        val error = replayError(UTF8_TYPE, sql to "not IPC")
        assertInstanceOf(Fault::class.java, error)
        assertEquals(sql, error.context("sql"))
        assertEquals(0, error.context("tx-op-idx"))
        assertNull(error.context("arg-idx"))
    }
}
