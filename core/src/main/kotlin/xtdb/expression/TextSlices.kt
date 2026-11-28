package xtdb.expression

import xtdb.InternalApi
import xtdb.arrow.ListVector
import xtdb.arrow.Utf8Vector
import xtdb.arrow.VariableWidthVector
import xtdb.arrow.isWellFormedUtf8
import xtdb.util.StringUtil
import java.nio.ByteBuffer
import java.nio.charset.Charset

/** @suppress */
@InternalApi
object TextSlices {
    private fun VariableWidthVector.writeSlice(input: ByteBuffer, start: Int, length: Int) {
        writeBytes(length) { dest, offset ->
            if (length > 0) dest.setBytes(offset, input.slice(start, length))
        }
    }

    @JvmStatic
    fun tryWriteSplit(out: ListVector, source: Any?, delimiter: Any?): Boolean {
        val input = source as? ByteBuffer ?: return false
        val separator = delimiter as? ByteBuffer ?: return false
        val elements = out.listElements as? Utf8Vector ?: return false
        if (!input.isWellFormedUtf8() || !separator.isWellFormedUtf8()) return false

        val size = input.remaining()
        if (size > 0) {
            val separatorSize = separator.remaining()
            val maxTokens = if (separatorSize == 0) 1L else size / separatorSize + 1L
            if (elements.valueCount + maxTokens > Int.MAX_VALUE ||
                elements.dataBuffer.writerIndex + size > Int.MAX_VALUE
            ) return false

            val tail = input.duplicate()
            if (separatorSize > 0) {
                while (true) {
                    val match = StringUtil.SqlBinPosition(separator, tail)
                    if (match == 0) break
                    val length = match - 1
                    elements.writeSlice(tail, tail.position(), length)
                    tail.position(tail.position() + length + separatorSize)
                }
            }
            elements.writeSlice(tail, tail.position(), tail.remaining())
        }
        out.endList()
        return true
    }

    @JvmStatic
    fun tryWriteAsciiChars(out: ListVector, source: Any?): Boolean {
        val input = source as? ByteBuffer ?: return false
        val elements = out.listElements as? Utf8Vector ?: return false
        val size = input.remaining()
        if (elements.valueCount + size.toLong() > Int.MAX_VALUE ||
            elements.dataBuffer.writerIndex + size > Int.MAX_VALUE ||
            !input.isAscii()
        ) return false

        for (index in input.position() until input.limit()) {
            elements.writeBytes(1) { dest, offset -> dest.setByte(offset, input[index].toInt()) }
        }
        out.endList()
        return true
    }

    @JvmStatic
    fun tryWriteKeywordPart(out: VariableWidthVector, input: ByteBuffer, namespace: Boolean): Boolean {
        if (Charset.defaultCharset() != Charsets.UTF_8 || !input.isWellFormedUtf8()) return false

        val start = input.position()
        val end = input.limit()
        var slash = -1
        for (index in start until end) {
            if (input[index] != '/'.code.toByte()) continue
            if (index == start || index == end - 1 || slash >= 0) return false
            slash = index
        }

        if (namespace) {
            if (slash < 0) out.writeNull() else out.writeSlice(input, start, slash - start)
        } else {
            val nameStart = if (slash < 0) start else slash + 1
            out.writeSlice(input, nameStart, end - nameStart)
        }
        return true
    }
}
