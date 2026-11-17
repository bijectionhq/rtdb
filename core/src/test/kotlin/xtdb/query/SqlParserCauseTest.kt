package xtdb.query

import clojure.lang.Keyword
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.InputMismatchException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.antlr.Sql
import xtdb.antlr.SqlLexer
import xtdb.api.error.Incorrect

class SqlParserCauseTest {
    @Test
    fun `statement mismatch exposes its live recognizer and shared decision states`() {
        val failure = assertThrows<Incorrect> { "SELEC 1".parseStatementAst() }
        assertEquals(Keyword.intern("xtdb/sql-error"), failure.data.valAt(Keyword.intern("xtdb.error", "code")))
        assertTrue(failure.message!!.startsWith("Errors parsing SQL statement:\n  - line 1:0 mismatched input 'SELEC' expecting "))

        val mismatch = assertInstanceOf(InputMismatchException::class.java, failure.cause)
        assertEquals(574, mismatch.offendingState)
        assertEquals(0, mismatch.offendingToken.tokenIndex)
        assertEquals("SELEC", mismatch.offendingToken.text)
        val context = assertInstanceOf(Sql.DirectlyExecutableStatementContext::class.java, mismatch.ctx)
        assertEquals(352, context.invokingState)
        assertSame(mismatch, context.exception)
        val parser = assertInstanceOf(Sql::class.java, mismatch.recognizer)
        val another = Sql(CommonTokenStream(SqlLexer(CharStreams.fromString("SELECT 1"))))
        assertSame(parser.inputStream, mismatch.inputStream)
        assertNotSame(parser, another)
        assertNotSame(parser.interpreter, another.interpreter)
        assertSame(parser.interpreter.decisionToDFA, another.interpreter.decisionToDFA)
        assertEquals(574, parser.atn.getDecisionState(34).stateNumber)
    }

    @Test
    fun `statement unwanted token is reported with a null cause`() {
        val failure = assertThrows<Incorrect> { "extra SELECT 1".parseStatementAst() }
        assertEquals(Keyword.intern("xtdb/sql-error"), failure.data.valAt(Keyword.intern("xtdb.error", "code")))
        assertTrue(failure.message!!.startsWith("Errors parsing SQL statement:\n  - line 1:0 extraneous input 'extra' expecting "))
        assertNull(failure.cause)
    }
}
