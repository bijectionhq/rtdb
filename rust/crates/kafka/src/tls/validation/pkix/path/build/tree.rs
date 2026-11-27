use std::cmp::Ordering;

struct Entry { value: usize, parent: Option<usize>, left: Option<usize>, right: Option<usize>, red: bool }
#[derive(Default)]
struct Tree { entries: Vec<Entry>, root: Option<usize> }

// OpenJDK 25.0.1 TreeMap.put/fixAfterInsertion: PKIXCertComparator's equal-preference result is not antisymmetric.
pub(super) fn ordered<E>(values: impl IntoIterator<Item = usize>, mut compare: impl FnMut(usize, usize) -> std::result::Result<Ordering, E>) -> std::result::Result<Vec<usize>, E> {
    let mut tree = Tree::default();
    for value in values { tree.insert(value, &mut compare)?; }
    let mut result = Vec::with_capacity(tree.entries.len());
    let mut pending = Vec::new();
    let mut next = tree.root;
    while next.is_some() || !pending.is_empty() {
        while let Some(index) = next { pending.push(index); next = tree.entries[index].left; }
        let index = pending.pop().expect("pending tree entry");
        result.push(tree.entries[index].value);
        next = tree.entries[index].right;
    }
    Ok(result)
}

impl Tree {
    fn insert<E>(&mut self, value: usize, compare: &mut impl FnMut(usize, usize) -> std::result::Result<Ordering, E>) -> std::result::Result<(), E> {
        let Some(mut parent) = self.root else {
            compare(value, value)?;
            self.root = Some(0);
            self.entries.push(Entry { value, parent: None, left: None, right: None, red: false });
            return Ok(());
        };
        let left = loop {
            let left = match compare(value, self.entries[parent].value)? {
                Ordering::Equal => return Ok(()),
                Ordering::Less => true,
                Ordering::Greater => false,
            };
            let next = if left { self.entries[parent].left } else { self.entries[parent].right };
            if let Some(next) = next { parent = next; } else { break left; }
        };
        let index = self.entries.len();
        self.entries.push(Entry { value, parent: Some(parent), left: None, right: None, red: true });
        if left { self.entries[parent].left = Some(index); } else { self.entries[parent].right = Some(index); }
        self.balance(index);
        Ok(())
    }

    fn rotate(&mut self, pivot: usize, left: bool) {
        let child = if left { self.entries[pivot].right } else { self.entries[pivot].left }.expect("rotation child");
        let inner = if left { self.entries[child].left } else { self.entries[child].right };
        if left { self.entries[pivot].right = inner; } else { self.entries[pivot].left = inner; }
        if let Some(inner) = inner { self.entries[inner].parent = Some(pivot); }
        let parent = self.entries[pivot].parent;
        self.entries[child].parent = parent;
        if let Some(parent) = parent {
            if self.entries[parent].left == Some(pivot) { self.entries[parent].left = Some(child); }
            else { self.entries[parent].right = Some(child); }
        } else { self.root = Some(child); }
        if left { self.entries[child].left = Some(pivot); } else { self.entries[child].right = Some(pivot); }
        self.entries[pivot].parent = Some(child);
    }

    fn balance(&mut self, mut index: usize) {
        while let Some(parent) = self.entries[index].parent.filter(|parent| self.entries[*parent].red) {
            let grand = self.entries[parent].parent.expect("red parent has a black ancestor");
            let left = self.entries[grand].left == Some(parent);
            let uncle = if left { self.entries[grand].right } else { self.entries[grand].left };
            if uncle.is_some_and(|uncle| self.entries[uncle].red) {
                self.entries[parent].red = false;
                self.entries[uncle.unwrap()].red = false;
                self.entries[grand].red = true;
                index = grand;
            } else {
                let inner = if left { self.entries[parent].right == Some(index) } else { self.entries[parent].left == Some(index) };
                if inner { index = parent; self.rotate(index, left); }
                let parent = self.entries[index].parent.expect("rebalanced parent");
                let grand = self.entries[parent].parent.expect("rebalanced grandparent");
                self.entries[parent].red = false;
                self.entries[grand].red = true;
                self.rotate(grand, !left);
            }
        }
        self.entries[self.root.expect("nonempty tree")].red = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparable_keys_follow_rotations_and_keep_first_equal_key() {
        assert_eq!(ordered([3, 1, 2, 0, 4, 2], |left, right| Ok::<_, ()>(left.cmp(&right))).unwrap(), [0, 1, 2, 3, 4]);
        assert_eq!(ordered([0, 4, 2, 3, 1], |left, right| Ok::<_, ()>(left.cmp(&right))).unwrap(), [0, 1, 2, 3, 4]);
    }

    #[test]
    fn equally_preferred_distinct_certificates_take_the_java_left_branch() {
        let compare = |left, right| Ok::<_, ()>(if left == right { Ordering::Equal } else { Ordering::Less });
        assert_eq!(ordered(0..6, compare).unwrap(), [5, 4, 3, 2, 1, 0]);
        assert_eq!(ordered([0, 1, 2, 3, 4, 5, 0], compare).unwrap(), [0, 5, 4, 3, 2, 1, 0]);
        let mut tree = Tree::default();
        for value in 0..6 { tree.insert(value, &mut { compare }).unwrap(); }
        assert_eq!(tree.root.map(|root| tree.entries[root].value), Some(1));
        assert!(tree.entries.iter().filter(|entry| entry.red).all(|entry| [entry.left, entry.right].into_iter().flatten().all(|child| !tree.entries[child].red)));
    }

    #[test]
    fn insertion_preserves_comparator_failures_including_the_first_key() {
        let mut compared = Vec::new();
        let result = ordered([2, 1, 3], |left, right| {
            compared.push((left, right));
            if left == 3 { Err(super::super::super::super::invalid("comparison failed")) } else { Ok(left.cmp(&right)) }
        });
        assert!(result.is_err());
        assert_eq!(compared, [(2, 2), (1, 2), (3, 2)]);
        assert!(ordered([0], |_, _| Err(super::super::super::super::invalid("first comparison failed"))).is_err());
    }
}
