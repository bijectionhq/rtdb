package xtdb.util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MsgIdUtilTest {
    @Test
    fun testRoundTripZeroEpoch() {
        val epoch = 0
        val offset = 123L
        val txId = MsgIdUtil.offsetToMsgId(epoch, offset)

        // Check that the txId is equal to the offset when epoch is 0
        assertEquals(offset, txId)

        assertEquals(epoch, MsgIdUtil.msgIdToEpoch(txId))
        assertEquals(offset, MsgIdUtil.msgIdToOffset(txId))
    }

    @Test
    fun testRoundTripNonZeroEpoch() {
        val epoch = 3
        val offset = 123L
        val txId = MsgIdUtil.offsetToMsgId(epoch, offset)

        assertEquals(epoch, MsgIdUtil.msgIdToEpoch(txId))
        assertEquals(offset, MsgIdUtil.msgIdToOffset(txId))
    }

    @Test
    fun maximumEpochPreservesOrderAndResume() {
        val epoch = Short.MAX_VALUE.toInt()
        val offset = (1L shl 48) - 1
        val lastInPreviousEpoch = MsgIdUtil.offsetToMsgId(epoch - 1, offset)
        val first = MsgIdUtil.offsetToMsgId(epoch, 0)
        val last = MsgIdUtil.offsetToMsgId(epoch, offset)

        assertTrue(lastInPreviousEpoch < first)
        assertEquals(Long.MAX_VALUE, last)
        assertEquals(epoch, MsgIdUtil.msgIdToEpoch(last))
        assertEquals(offset, MsgIdUtil.afterMsgIdToOffset(epoch, last))
        assertEquals(-1L, MsgIdUtil.afterMsgIdToOffset(epoch, lastInPreviousEpoch))
    }

    @Test
    fun rejectsEpochsOutsideTheMessageIdRange() {
        for (epoch in listOf(Int.MIN_VALUE, -1, 32768, 65535, Int.MAX_VALUE)) {
            assertThrows(IllegalArgumentException::class.java) { MsgIdUtil.offsetToMsgId(epoch, 0) }
            assertThrows(IllegalArgumentException::class.java) { MsgIdUtil.offsetToMsgId(epoch, -1) }
        }
    }
}
