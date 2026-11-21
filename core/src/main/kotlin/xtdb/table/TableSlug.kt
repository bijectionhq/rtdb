package xtdb.table

import xtdb.api.TableRef
import xtdb.trie.TrieKey
import xtdb.util.asPath
import java.nio.file.Path

/**
 * The directory under `tables/` holding one table's files.
 *
 * A table's location, never its identity: it is frozen when the table is first recorded and carried
 * forward verbatim from block to block, so renaming a table changes its name and leaves this alone.
 * Deriving one from a name is therefore only correct for a table that has no recorded slug — see [of].
 */
data class TableSlug(val slug: String) {

    val tablePath: Path get() = tablesDir.resolve(slug)

    fun dataFileDir(): Path = tablePath.resolve("data")

    fun dataFilePath(trieKey: TrieKey): Path = dataFileDir().resolve("$trieKey.arrow")

    fun metaFileDir(): Path = tablePath.resolve("meta")

    fun metaFilePath(trieKey: TrieKey): Path = metaFileDir().resolve("$trieKey.arrow")

    companion object {
        @JvmStatic
        val tablesDir = "tables".asPath

        /**
         * The escaped name used by checkpoints that predate the registry and the default for newly allocated tables.
         *
         * A names-only checkpoint uses this exact path to locate its existing files.
         * The catalog gives a new table a free suffix when this default is already in use.
         */
        @JvmStatic
        fun of(table: TableRef) =
            TableSlug("${table.schemaName}$${table.tableName}".replace(Regex("[./]"), "\\$"))
    }
}
