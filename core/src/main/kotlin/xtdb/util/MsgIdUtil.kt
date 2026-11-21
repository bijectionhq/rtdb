package xtdb.util

import xtdb.types.LogOffset
import xtdb.types.MessageId

object MsgIdUtil {
    private const val EPOCH_LIMIT = 1 shl 15
    private const val OFFSET_LIMIT = 1L shl 48
    private const val OFFSET_MASK = OFFSET_LIMIT - 1

    @JvmStatic
    fun offsetToMsgId(epoch: Int, offset: LogOffset): MessageId {
        require(epoch in 0 until EPOCH_LIMIT) { "Log epoch ($epoch) must be in 0..${EPOCH_LIMIT - 1}" }
        require(offset < OFFSET_LIMIT) { "Offset value ($offset) exceeds the limit ($OFFSET_LIMIT)" }
        return (epoch.toLong() shl 48) + offset
    }

    @JvmStatic
    fun msgIdToEpoch(msgId: Long): Int = (msgId shr 48).toInt()

    @JvmStatic
    fun msgIdToOffset(msgId: Long): MessageId = msgId and OFFSET_MASK

    /**
     * Converts an "after" MessageId to a LogOffset for a given epoch.
     * If the msg-id is from a different epoch, returns -1 (start from beginning).
     */
    @JvmStatic
    fun afterMsgIdToOffset(epoch: Int, afterMsgId: MessageId): LogOffset = when {
        afterMsgId < 0 -> -1L
        msgIdToEpoch(afterMsgId) == epoch -> msgIdToOffset(afterMsgId)
        else -> -1L
    }
}
