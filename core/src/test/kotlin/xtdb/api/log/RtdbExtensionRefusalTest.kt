package xtdb.api.log

import com.google.protobuf.InvalidProtocolBufferException
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.block.proto.Block
import xtdb.block.proto.TableBlock

/**
 * The bytes RTDB writes for records and block files that use an RTDB-only feature, as pinned by
 * `rust/crates/format/tests/xtdb_refusal.rs` (`xtdb_reads_these_exact_bytes`), fail this codebase's decoders
 * instead of being skipped. See dev/doc/db.allium (XtdbRefusesRtdbExtensionRecords).
 */
class RtdbExtensionRefusalTest {

    private fun bytes(vararg values: Int) = ByteArray(values.size) { values[it].toByte() }

    // TablesRetired { table_names = ["a/b"], as_of_micros = 7 }, term 3, sequence 5.
    private val tablesRetiredBody = intArrayOf(0xd2, 0x3e, 0x07, 0x0a, 0x03, 0x61, 0x2f, 0x62, 0x10, 0x07, 0x38, 0x03, 0x48, 0x05)

    @Test
    fun `an unframed RTDB-only replica record is skipped silently, which is why RTDB frames it`() {
        assertNull(ReplicaMessage.decode(bytes(0x03, *tablesRetiredBody)))
    }

    @Test
    fun `a framed RTDB-only replica record fails to parse`() {
        val error = assertThrows<InvalidProtocolBufferException> {
            ReplicaMessage.decode(bytes(0x04, 0x00, *tablesRetiredBody))
        }
        assertTrue(error.message!!.contains("invalid tag (zero)"), error.message)
    }

    @Test
    fun `a framed RTDB-only source record is rejected by its header`() {
        val error = assertThrows<IllegalArgumentException> {
            SourceMessage.decode(bytes(0x04, 0xd2, 0x3e, 0x02, 0x09, 0x09))
        }
        assertTrue(error.message!!.contains("Unknown message type: 4"), error.message)
    }

    @Test
    fun `framed RTDB-only block files fail to parse`() {
        assertThrows<InvalidProtocolBufferException> { Block.parseFrom(bytes(0x00, 0x08, 0x01, 0xc0, 0x3e, 0x01)) }
        assertThrows<InvalidProtocolBufferException> { TableBlock.parseFrom(bytes(0x00, 0x10, 0x01, 0xd0, 0x3e, 0x07)) }
        Block.parseFrom(bytes(0x08, 0x01, 0xc0, 0x3e, 0x01))
        TableBlock.parseFrom(bytes(0x10, 0x01, 0xd0, 0x3e, 0x07))
    }
}
