package xtdb.catalog

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.TableRef
import xtdb.table.TableSlug

class TableCatalogTest {
    @Test
    fun catalogAllocatesTheFirstFreeSuffixForCollidingSlugs() {
        val original = TableRef("public", "a\$b")
        val suffixOwner = TableRef("public", "a\$b~1")
        val dotted = TableRef("public", "a.b")
        val catalog = TableCatalog.State(null, emptyMap(), emptyMap()).withEntriesFor(listOf(suffixOwner, original))
        val next = catalog.withEntriesFor(listOf(dotted, dotted))

        assertEquals(catalog.entry(original), next.entry(original))
        assertEquals(catalog.entry(suffixOwner), next.entry(suffixOwner))
        assertEquals(TableSlug("public\$a\$b~2"), next.entry(dotted)?.slug)
        assertEquals(3, next.entries.size)
    }
}
