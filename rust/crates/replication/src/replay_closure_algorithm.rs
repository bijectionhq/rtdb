macro_rules! checkpoint_dependency_keys {
    ($catalog:ident, $tables:ident, $contents:ident; $each:ident) => {{
        let block = $catalog.block_index();
        let mut keys = BTreeSet::new();
        keys.insert(ObjectKey::new(block_key(block)?)?);
        $each!(keys, &$catalog.table_entries()?, entry => {
            let table = $tables.get(&entry.table).ok_or_else(|| invalid("A checkpoint table has no table block"))?;
            keys.insert(ObjectKey::new(entry.slug.block_key(block)?)?);
            let files = table.checkpoint_trie_files(entry, $contents)?;
            insert_object_keys(&mut keys, files)?;
        }, [|entry: TableEntry| tableKeys(block, entry, $tables)]);
        Ok(keys)
    }};
}

macro_rules! record_dependency_keys {
    ($base:ident, $body:ident, $slugs:ident, $keys:ident; $each:ident) => {{
        match $body {
            ReplicaBody::BlockUploaded(upload) => {
                if adopted_upload(&$base, $body).is_some() { added_trie_keys($slugs, &upload.tries, $keys)?; }
            }
            ReplicaBody::TriesAdded(message) => {
                if same_storage(&$base, message.storage_version, message.storage_epoch) {
                    added_trie_keys($slugs, &message.tries, $keys)?;
                }
            }
            ReplicaBody::ArtefactsAdded(message) => {
                if same_storage(&$base, message.storage_version, message.storage_epoch) {
                    if let Ok(table) = legacy_table_ref_from_wire(&message.table_name) {
                        $each!($keys, table_slugs($slugs, &table)?, slug => {
                            $each!($keys, &artefact_entries(&message.entries), entry => {
                                let (trie, record) = entry;
                                if let Some(key) = replay_artefact_file(slug, trie, record)? { $keys.insert(ObjectKey::new(key)?); }
                            }, [|entry: (TrieKey, ArtefactRecord)| artefactEntryKeys(*slug, entry)]);
                        }, [|slug: TableSlug| artefactKeysAt(slug, message)]);
                    }
                }
            }
            ReplicaBody::ResolvedTx(_) | ReplicaBody::NoOp(_) | ReplicaBody::BlockBoundary(_)
                | ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) | ReplicaBody::ArtefactsRemoved(_) => {}
            ReplicaBody::OversizedMessage(_) => return Err(invalid("An oversized payload holds another oversized message")),
        }
    }};
}

macro_rules! added_trie_dependency_keys {
    ($slugs:ident, $tries:ident, $keys:ident; $each:ident) => {{
        $each!($keys, $tries, trie => {
            let table = legacy_table_ref_from_wire(&trie.table_name)?;
            $each!($keys, table_slugs($slugs, &table)?, slug => {
                let files = trie.checkpoint_files(slug, CheckpointContents::Replay)?;
                insert_object_keys($keys, files)?;
            }, [|slug: TableSlug| trieKeys(slug, *trie, CheckpointContents::Replay)]);
        }, [|trie: TrieDetails| addedTrieKeys(trie, $slugs.entries@)]);
        Ok(())
    }};
}

macro_rules! oversized_dependency {
    ($envelope:ident, $base:ident, $keys:ident, $bodies:ident, $read:ident) => {{
        match $envelope.oversized_payload($base.storage_version, $base.storage_epoch)? {
            Some(path) => {
                let key = ObjectKey::new(path)?;
                let payload = $read!(&key)?;
                $bodies.push(Cow::Owned($envelope.unwrap_payload(&payload)?.message));
                $keys.insert(key);
            }
            None => $bodies.push(Cow::Borrowed(&$envelope.message)),
        }
    }};
}

macro_rules! adopt_dependency {
    ($catalog:ident, $tables:ident, $keys:ident, $block:ident) => {{
        if $catalog.block_index() != $block { return Err(invalid("An adopted checkpoint has another block index")); }
        $keys.append(&mut checkpoint_dependencies($catalog, $tables)?);
    }};
}

macro_rules! checked_object_keys {
    ($keys:ident, $files:ident; $each:ident) => {{
        $each!($keys, $files, file => {
            $keys.insert(ObjectKey::new(file)?);
        }, [|file: u64| ISet::empty().insert(file)]);
        Ok(())
    }};
}

macro_rules! matching_replay_storage {
    ($version:ident, $epoch:ident, $base_version:expr, $base_epoch:expr) => {
        $version.unwrap_or(0) == $base_version && $epoch.unwrap_or(0) == $base_epoch
    };
}

macro_rules! new_upload {
    ($base:ident, $body:ident, $installed:expr) => {{
        let ReplicaBody::BlockUploaded(upload) = $body else { return None; };
        if same_storage($base, upload.storage_version, upload.storage_epoch)
            && upload.block_index.unwrap_or(0) > $installed
        {
            Some(upload)
        } else {
            None
        }
    }};
}

macro_rules! tail_dependency_keys {
    ($base:ident, $suffix:ident, $checkpoint:expr;
        $each_resolved:ident, $each_adopted:ident, $each_body:ident,
        $borrow:ident, $resolve:ident, $read_catalog:ident
    ) => {{
        let mut keys = BTreeSet::new();
        let mut bodies = Vec::with_capacity($suffix.len());
        $each_resolved!(keys, bodies, &$suffix, input => {
            let envelope = $borrow!(input);
            let keys = &mut keys;
            let bodies = &mut bodies;
            $resolve!(envelope, $base, keys, bodies);
        });

        let mut slugs = Slugs::new();
        if let Some(checkpoint) = $checkpoint { record_slugs(&mut slugs, checkpoint)?; }
        let mut adopted = AdoptedCatalogs::new();
        $each_adopted!(keys, slugs, adopted, &bodies, $base, input => {
            if let Some(upload) = adopted_upload(&$base, input.as_ref()) {
                let block = upload_block(upload);
                if let Some(catalog) = adopted.get(&block) {
                    validate_uploaded_root(catalog, upload, false)?;
                } else {
                    let (catalog, tables) = $read_catalog!(block)?;
                    adopt_catalog(&catalog, &tables, &mut keys, block)?;
                    record_slugs(&mut slugs, &catalog)?;
                    validate_uploaded_root(&catalog, upload, false)?;
                    adopted.insert(block, catalog);
                }
            }
        });

        $each_body!(keys, slugs, &bodies, input => {
            let body = input.as_ref();
            record_dependency_keys($base, body, &slugs, &mut keys)?;
        }, [|input: Cow| bodyFileKeys($base, &value(input), slugs.entries@)]);
        Ok(keys)
    }};
}

macro_rules! record_catalog_slugs {
    ($slugs:ident, $catalog:ident; $each:ident, $insert:ident) => {{
        $each!($slugs, $catalog.table_entries()?, entry => {
            $insert!($slugs, entry);
        }, [|entry: TableEntry| ISet::empty().insert(entry)]);
        Ok(())
    }};
}
