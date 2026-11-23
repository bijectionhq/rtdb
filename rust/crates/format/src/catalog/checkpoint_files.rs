macro_rules! trie_checkpoint_files {
    ($detail:ident, $slug:ident, $contents:ident; $each:ident) => {{
        let state = checkpoint_trie_state($detail)?;
        let trie: TrieKey = checkpoint_trie_key($detail)?;
        let mut files = BTreeSet::new();
        if matches!(state, TrieState::Garbage)
            && (matches!($contents, CheckpointContents::Primary) || trie.level() != 0)
        {
            return Ok(files);
        }
        files.insert($slug.data_key(&trie));
        files.insert($slug.metadata_key(&trie));
        if matches!($contents, CheckpointContents::Replay) && !matches!(state, TrieState::Garbage) {
            $each!(files, &$detail.artefacts, record => {
                if let Some(key) = replay_artefact_file($slug, &trie, record)? { files.insert(key); }
            }, [|record: ArtefactRecord| ISet::empty().insert(artefactKey(*$slug, trie, record))]);
        }
        Ok(files)
    }};
}

macro_rules! table_checkpoint_files {
    ($table:ident, $entry:ident, $contents:ident; $each:ident) => {{
        let mut files = BTreeSet::new();
        $each!(files, $table.trie_details(), detail => {
            if !($entry.table.stores_expiring_transaction_metadata()
                && detail.trie_state == Some(TrieState::Garbage as i32))
            {
                files.append(&mut detail.checkpoint_files(&$entry.slug, $contents)?);
            }
        }, [|detail: TrieDetails| retainedTrieKeys($entry, detail, $contents)]);
        if matches!($contents, CheckpointContents::Replay) {
            $each!(files, &$table.wire.search_runs, run => {
                files.insert(run.object_key(&$entry.slug)?);
            }, [|run: SearchRun| ISet::empty().insert(searchRunKey($entry.slug, run))]);
        }
        Ok(files)
    }};
}
