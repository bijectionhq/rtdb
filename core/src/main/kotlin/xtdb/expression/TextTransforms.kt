package xtdb.expression

import xtdb.InternalApi
import xtdb.arrow.VariableWidthVector
import xtdb.arrow.isWellFormedUtf8
import xtdb.util.StringUtil
import java.nio.ByteBuffer
import java.util.Locale

/** @suppress */
@InternalApi
object TextTransforms {
    private fun ByteBuffer.isAscii(): Boolean {
        for (idx in position() until limit()) {
            if (get(idx) < 0) return false
        }
        return true
    }

    @JvmStatic
    fun tryWriteAsciiCase(out: VariableWidthVector, value: Any?, upper: Boolean): Boolean {
        val input = value as? ByteBuffer ?: return false
        when (Locale.getDefault().language) {
            "tr", "az", "lt" -> return false
        }
        if (!input.isAscii()) return false

        val first = if (upper) 'a'.code else 'A'.code
        val last = if (upper) 'z'.code else 'Z'.code
        out.writeBytes(input.remaining()) { dest, start ->
            var offset = start
            for (idx in input.position() until input.limit()) {
                val octet = input[idx].toInt()
                dest.setByte(offset++, if (octet in first..last) octet xor 0x20 else octet)
            }
        }
        return true
    }

    @JvmStatic
    fun tryWriteAsciiReverse(out: VariableWidthVector, value: Any?): Boolean {
        val input = value as? ByteBuffer ?: return false
        if (!input.isAscii()) return false

        out.writeBytes(input.remaining()) { dest, start ->
            var offset = start
            for (idx in input.limit() - 1 downTo input.position()) dest.setByte(offset++, input[idx].toInt())
        }
        return true
    }

    @JvmStatic
    fun tryWriteLiteralReplace(out: VariableWidthVector, value: Any?, target: ByteBuffer, replacement: ByteArray): Boolean {
        val input = value as? ByteBuffer ?: return false
        if (!target.hasRemaining() || !input.isWellFormedUtf8()) return false

        val cursor = input.duplicate()
        val targetSize = target.remaining()
        var matches = 0
        while (true) {
            val match = StringUtil.SqlBinPosition(target, cursor)
            if (match == 0) break
            cursor.position(cursor.position() + (match - 1) + targetSize)
            matches++
        }

        val size = input.remaining() + matches.toLong() * (replacement.size - targetSize)
        if (size > Int.MAX_VALUE) return false

        cursor.position(input.position())
        out.writeBytes(size.toInt()) { dest, destinationStart ->
            var offset = destinationStart
            repeat(matches) {
                val prefixSize = StringUtil.SqlBinPosition(target, cursor) - 1
                val matchStart = cursor.position() + prefixSize
                if (prefixSize > 0) {
                    cursor.limit(matchStart)
                    dest.setBytes(offset, cursor)
                    cursor.limit(input.limit())
                    offset += prefixSize
                }
                if (replacement.isNotEmpty()) dest.setBytes(offset, replacement)
                offset += replacement.size
                cursor.position(matchStart + targetSize)
            }
            if (cursor.hasRemaining()) dest.setBytes(offset, cursor)
        }
        return true
    }

    private fun asciiHexDigit(octet: Int): Int = when (octet) {
        in '0'.code..'9'.code -> octet - '0'.code
        in 'a'.code..'f'.code -> octet - 'a'.code + 10
        in 'A'.code..'F'.code -> octet - 'A'.code + 10
        else -> -1
    }

    @JvmStatic
    fun tryWriteHexDecoded(out: VariableWidthVector, value: Any?): Boolean {
        val input = value as? ByteBuffer ?: return false
        val size = input.remaining()
        if (size % 2 != 0) return false

        val start = input.position()
        val end = input.limit()
        for (idx in start until end) {
            if (asciiHexDigit(input[idx].toInt()) < 0) return false
        }

        out.writeBytes(size / 2) { dest, destinationStart ->
            var offset = destinationStart
            for (idx in start until end step 2) {
                val hi = asciiHexDigit(input[idx].toInt())
                val lo = asciiHexDigit(input[idx + 1].toInt())
                dest.setByte(offset++, (hi shl 4) or lo)
            }
        }
        return true
    }

    @JvmStatic
    fun tryWriteHexEncoded(out: VariableWidthVector, value: Any?): Boolean {
        val input = value as? ByteBuffer ?: return false
        val hasArray = input.hasArray()
        if (hasArray && (input.arrayOffset() != 0 || input.remaining() != input.array().size)) return false

        val size = input.remaining() * 2L + 2
        if (size > Int.MAX_VALUE) return false

        val hex = "0123456789abcdef"
        out.writeBytes(size.toInt()) { dest, start ->
            dest.setByte(start, '\\'.code)
            dest.setByte(start + 1, 'x'.code)
            var offset = start + 2
            for (idx in input.position() until input.limit()) {
                val octet = (if (hasArray) input[idx] else input.get()).toInt() and 0xff
                dest.setByte(offset++, hex[octet ushr 4].code)
                dest.setByte(offset++, hex[octet and 0xf].code)
            }
        }
        return true
    }
}
