use super::*;

impl<K, V, L> Map<K, V, L> {
    pub(super) fn tree_find<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, mut current: usize, hash: i32, key: &K, mut comparable: Option<O::Comparable>, ops: &mut O, admission: &mut A) -> Result<Option<usize>> {
        loop {
            let entry = self.entry(current); let left = entry.links.left; let right = entry.links.right;
            let next = if entry.hash > hash { left } else if entry.hash < hash { right }
            else if Self::key_equal(key, &entry.key, ops, admission)? { return Ok(Some(current)); }
            else if left.is_none() { right } else if right.is_none() { left }
            else {
                if comparable.is_none() { comparable = ops.comparable_class(key, admission)?; }
                let direction = match comparable { Some(class) => ops.compare(class, key, &entry.key, admission)?, None => 0 };
                if direction != 0 { if direction < 0 { left } else { right } }
                else {
                    if let Some(found) = self.tree_find(right.unwrap(), hash, key, comparable, ops, admission)? { return Ok(Some(found)); }
                    left
                }
            };
            match next { Some(next) => current = next, None => return Ok(None) }
        }
    }
    pub(super) fn tie_break<A: Admission<Lease = L>, O: KeyOps<K, A>>(key: &K, stored: &K, ops: &mut O, admission: &mut A) -> Result<Ordering> {
        if !ops.is_null(key) && !ops.is_null(stored) {
            let classes = ops.class_name(key).encode_utf16().cmp(ops.class_name(stored).encode_utf16());
            if classes != Ordering::Equal { return Ok(classes); }
        }
        let left = if ops.is_null(key) { 0 } else { ops.identity_hash_code(key, admission)? };
        let right = if ops.is_null(stored) { 0 } else { ops.identity_hash_code(stored, admission)? };
        Ok(if left <= right { Ordering::Less } else { Ordering::Greater })
    }
    pub(super) fn tree_put<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, mut current: usize, hash: i32, key: &K, ops: &mut O, admission: &mut A) -> Result<TreePut> {
        let mut comparable = None; let mut searched = false;
        loop {
            let entry = self.entry(current);
            let direction = if entry.hash > hash { Ordering::Less } else if entry.hash < hash { Ordering::Greater }
            else if Self::key_equal(key, &entry.key, ops, admission)? { return Ok(TreePut::Existing(current)); }
            else {
                if comparable.is_none() { comparable = ops.comparable_class(key, admission)?; }
                let compared = match comparable { Some(class) => ops.compare(class, key, &entry.key, admission)?, None => 0 };
                if compared != 0 { compared.cmp(&0) }
                else {
                    if !searched {
                        searched = true;
                        if let Some(left) = entry.links.left { if let Some(found) = self.tree_find(left, hash, key, comparable, ops, admission)? { return Ok(TreePut::Existing(found)); } }
                        if let Some(right) = entry.links.right { if let Some(found) = self.tree_find(right, hash, key, comparable, ops, admission)? { return Ok(TreePut::Existing(found)); } }
                    }
                    Self::tie_break(key, &entry.key, ops, admission)?
                }
            };
            let left = direction != Ordering::Greater;
            match if left { entry.links.left } else { entry.links.right } { Some(next) => current = next, None => return Ok(TreePut::Vacant { parent: current, left }) }
        }
    }
    pub(super) fn treeify<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, bucket: &mut Bucket, ops: &mut O, admission: &mut A) -> Result<()> {
        let Some(mut root) = bucket.head else { return Ok(()); };
        self.clear_tree(bucket);
        let mut next = self.entry(root).links.next;
        while let Some(index) = next {
            next = self.entry(index).links.next; let hash = self.entry(index).hash; let mut comparable = None; let mut parent = root;
            loop {
                let stored = self.entry(parent);
                let direction = if stored.hash > hash { Ordering::Less } else if stored.hash < hash { Ordering::Greater }
                else {
                    let key = &self.entry(index).key;
                    if comparable.is_none() { comparable = ops.comparable_class(key, admission)?; }
                    let compared = match comparable { Some(class) => ops.compare(class, key, &stored.key, admission)?, None => 0 };
                    if compared != 0 { compared.cmp(&0) } else { Self::tie_break(key, &stored.key, ops, admission)? }
                };
                let left = direction != Ordering::Greater;
                match if left { stored.links.left } else { stored.links.right } {
                    Some(next) => parent = next,
                    None => {
                        self.entry_mut(index).links.parent = Some(parent);
                        if left { self.entry_mut(parent).links.left = Some(index); } else { self.entry_mut(parent).links.right = Some(index); }
                        root = self.balance_insertion(root, index); break;
                    }
                }
            }
        }
        self.front(bucket, root); Ok(())
    }
    pub(super) fn rotate(&mut self, mut root: usize, pivot: usize, left: bool) -> usize {
        let child = if left { self.entry(pivot).links.right } else { self.entry(pivot).links.left }.unwrap();
        let inner = if left { self.entry(child).links.left } else { self.entry(child).links.right };
        if left { self.entry_mut(pivot).links.right = inner; } else { self.entry_mut(pivot).links.left = inner; }
        if let Some(inner) = inner { self.entry_mut(inner).links.parent = Some(pivot); }
        let parent = self.entry(pivot).links.parent; self.entry_mut(child).links.parent = parent;
        if let Some(parent) = parent { if self.entry(parent).links.left == Some(pivot) { self.entry_mut(parent).links.left = Some(child); } else { self.entry_mut(parent).links.right = Some(child); } }
        else { root = child; self.entry_mut(child).links.red = false; }
        if left { self.entry_mut(child).links.left = Some(pivot); } else { self.entry_mut(child).links.right = Some(pivot); }
        self.entry_mut(pivot).links.parent = Some(child); root
    }
    pub(super) fn balance_insertion(&mut self, mut root: usize, mut index: usize) -> usize {
        self.entry_mut(index).links.red = true;
        loop {
            let Some(parent) = self.entry(index).links.parent else { self.entry_mut(index).links.red = false; return index; };
            let Some(grand) = self.entry(parent).links.parent else { return root; };
            if !self.entry(parent).links.red { return root; }
            let left = self.entry(grand).links.left == Some(parent); let uncle = if left { self.entry(grand).links.right } else { self.entry(grand).links.left };
            if uncle.is_some_and(|uncle| self.entry(uncle).links.red) {
                self.entry_mut(uncle.unwrap()).links.red = false; self.entry_mut(parent).links.red = false; self.entry_mut(grand).links.red = true; index = grand;
            } else {
                let triangle = if left { self.entry(parent).links.right == Some(index) } else { self.entry(parent).links.left == Some(index) };
                if triangle { index = parent; root = self.rotate(root, index, left); }
                if let Some(parent) = self.entry(index).links.parent {
                    self.entry_mut(parent).links.red = false;
                    if let Some(grand) = self.entry(parent).links.parent { self.entry_mut(grand).links.red = true; root = self.rotate(root, grand, !left); }
                }
            }
        }
    }
}
