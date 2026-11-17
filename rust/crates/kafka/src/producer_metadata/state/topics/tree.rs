use super::*;
use std::cmp::Ordering;

impl ActiveTopics {
    pub(super) fn direction(&self, hash: usize, name: &Text, stored: usize) -> Ordering {
        let node = self.node(stored);
        hash.cmp(&node.hash).then_with(|| name.as_str().encode_utf16().cmp(node.name.as_str().encode_utf16()))
    }
    pub(super) fn tree_find(&self, mut current: usize, hash: usize, name: &Text) -> Option<usize> {
        loop {
            let node = self.node(current);
            let next = match self.direction(hash, name, current) {
                Ordering::Less => node.links.left,
                Ordering::Greater => node.links.right,
                Ordering::Equal => return Some(current),
            };
            current = next?;
        }
    }
    pub(super) fn tree_position(&self, mut current: usize, hash: usize, name: &Text) -> std::result::Result<usize, (usize, bool)> {
        loop {
            let left = match self.direction(hash, name, current) {
                Ordering::Equal => return Ok(current),
                Ordering::Less => true,
                Ordering::Greater => false,
            };
            match if left { self.node(current).links.left } else { self.node(current).links.right } {
                Some(next) => current = next,
                None => return Err((current, left)),
            }
        }
    }
    pub(super) fn build_tree(&mut self, first: usize) -> usize {
        let mut root = first;
        let mut current = Some(first);
        while let Some(index) = current {
            let links = self.node(index).links;
            current = links.next;
            self.node_mut(index).links = Links { previous: links.previous, next: links.next, ..Links::default() };
            if index == first { continue; }
            let mut parent = root;
            loop {
                let left = self.direction(self.node(index).hash, &self.node(index).name, parent) != Ordering::Greater;
                let next = if left { self.node(parent).links.left } else { self.node(parent).links.right };
                if let Some(next) = next { parent = next; }
                else {
                    self.node_mut(index).links.parent = Some(parent);
                    if left { self.node_mut(parent).links.left = Some(index); } else { self.node_mut(parent).links.right = Some(index); }
                    root = self.balance_insertion(root, index);
                    break;
                }
            }
        }
        root
    }
    pub(super) fn remove_tree(&mut self, mut first: usize, mut root: usize, index: usize) -> Bin {
        let links = self.node(index).links;
        if let Some(previous) = links.previous { self.node_mut(previous).links.next = links.next; }
        else { let Some(next) = links.next else { return Bin::Empty; }; first = next; }
        if let Some(next) = links.next { self.node_mut(next).links.previous = links.previous; }
        let left = self.node(root).links.left;
        if self.node(root).links.right.is_none() || left.is_none() || self.node(left.unwrap()).links.left.is_none() {
            return self.list_bin(Some(first));
        }
        let replacement = match (links.left, links.right) {
            (Some(left), Some(right)) => {
                let mut successor = right;
                while let Some(left) = self.node(successor).links.left { successor = left; }
                let successor_links = self.node(successor).links;
                self.node_mut(successor).links.red = links.red;
                self.node_mut(index).links.red = successor_links.red;
                if successor == right {
                    self.node_mut(index).links.parent = Some(successor);
                    self.node_mut(successor).links.right = Some(index);
                } else {
                    self.node_mut(index).links.parent = successor_links.parent;
                    if let Some(parent) = successor_links.parent {
                        if self.node(parent).links.left == Some(successor) { self.node_mut(parent).links.left = Some(index); }
                        else { self.node_mut(parent).links.right = Some(index); }
                    }
                    self.node_mut(successor).links.right = Some(right);
                    self.node_mut(right).links.parent = Some(successor);
                }
                self.node_mut(index).links.left = None;
                self.node_mut(index).links.right = successor_links.right;
                if let Some(right) = successor_links.right { self.node_mut(right).links.parent = Some(index); }
                self.node_mut(successor).links.left = Some(left);
                self.node_mut(left).links.parent = Some(successor);
                self.node_mut(successor).links.parent = links.parent;
                if let Some(parent) = links.parent {
                    if self.node(parent).links.left == Some(index) { self.node_mut(parent).links.left = Some(successor); }
                    else { self.node_mut(parent).links.right = Some(successor); }
                } else { root = successor; }
                successor_links.right.unwrap_or(index)
            }
            (Some(child), None) | (None, Some(child)) => child,
            (None, None) => index,
        };
        if replacement != index {
            let parent = self.node(index).links.parent;
            self.node_mut(replacement).links.parent = parent;
            if let Some(parent) = parent {
                if self.node(parent).links.left == Some(index) { self.node_mut(parent).links.left = Some(replacement); }
                else { self.node_mut(parent).links.right = Some(replacement); }
            } else { root = replacement; }
            let links = &mut self.node_mut(index).links;
            links.left = None; links.right = None; links.parent = None;
        }
        root = if self.node(index).links.red { root } else { self.balance_deletion(root, Some(replacement)) };
        if replacement == index {
            let parent = self.node(index).links.parent;
            self.node_mut(index).links.parent = None;
            if let Some(parent) = parent {
                if self.node(parent).links.left == Some(index) { self.node_mut(parent).links.left = None; }
                else if self.node(parent).links.right == Some(index) { self.node_mut(parent).links.right = None; }
            }
        }
        Bin::Tree { first, root }
    }
    pub(super) fn rotate(&mut self, mut root: usize, pivot: usize, left: bool) -> usize {
        let child = if left { self.node(pivot).links.right } else { self.node(pivot).links.left }.unwrap();
        let inner = if left { self.node(child).links.left } else { self.node(child).links.right };
        if left { self.node_mut(pivot).links.right = inner; } else { self.node_mut(pivot).links.left = inner; }
        if let Some(inner) = inner { self.node_mut(inner).links.parent = Some(pivot); }
        let parent = self.node(pivot).links.parent; self.node_mut(child).links.parent = parent;
        if let Some(parent) = parent { if self.node(parent).links.left == Some(pivot) { self.node_mut(parent).links.left = Some(child); } else { self.node_mut(parent).links.right = Some(child); } }
        else { root = child; self.node_mut(child).links.red = false; }
        if left { self.node_mut(child).links.left = Some(pivot); } else { self.node_mut(child).links.right = Some(pivot); }
        self.node_mut(pivot).links.parent = Some(child); root
    }
    pub(super) fn balance_insertion(&mut self, mut root: usize, mut index: usize) -> usize {
        self.node_mut(index).links.red = true;
        loop {
            let Some(parent) = self.node(index).links.parent else { self.node_mut(index).links.red = false; return index; };
            let Some(grand) = self.node(parent).links.parent else { return root; };
            if !self.node(parent).links.red { return root; }
            let left = self.node(grand).links.left == Some(parent); let uncle = if left { self.node(grand).links.right } else { self.node(grand).links.left };
            if uncle.is_some_and(|uncle| self.node(uncle).links.red) {
                self.node_mut(uncle.unwrap()).links.red = false; self.node_mut(parent).links.red = false; self.node_mut(grand).links.red = true; index = grand;
            } else {
                let triangle = if left { self.node(parent).links.right == Some(index) } else { self.node(parent).links.left == Some(index) };
                if triangle { index = parent; root = self.rotate(root, index, left); }
                if let Some(parent) = self.node(index).links.parent {
                    self.node_mut(parent).links.red = false;
                    if let Some(grand) = self.node(parent).links.parent { self.node_mut(grand).links.red = true; root = self.rotate(root, grand, !left); }
                }
            }
        }
    }
    pub(super) fn balance_deletion(&mut self, mut root: usize, mut current: Option<usize>) -> usize {
        loop {
            let Some(index) = current else { return root; };
            if index == root { return root; }
            let Some(parent) = self.node(index).links.parent else { self.node_mut(index).links.red = false; return index; };
            if self.node(index).links.red { self.node_mut(index).links.red = false; return root; }
            let left = self.node(parent).links.left == Some(index);
            let mut parent = Some(parent);
            let mut sibling = if left { self.node(parent.unwrap()).links.right } else { self.node(parent.unwrap()).links.left };
            if self.removal_red(sibling) {
                self.node_mut(sibling.unwrap()).links.red = false;
                self.node_mut(parent.unwrap()).links.red = true;
                root = self.rotate(root, parent.unwrap(), left);
                parent = self.node(index).links.parent;
                sibling = parent.and_then(|parent| if left { self.node(parent).links.right } else { self.node(parent).links.left });
            }
            let Some(sibling_index) = sibling else { current = parent; continue; };
            let sibling_links = self.node(sibling_index).links;
            if !self.removal_red(sibling_links.left) && !self.removal_red(sibling_links.right) {
                self.node_mut(sibling_index).links.red = true;
                current = parent;
            } else {
                let outer = if left { sibling_links.right } else { sibling_links.left };
                if !self.removal_red(outer) {
                    let inner = if left { sibling_links.left } else { sibling_links.right };
                    if let Some(inner) = inner { self.node_mut(inner).links.red = false; }
                    self.node_mut(sibling_index).links.red = true;
                    root = self.rotate(root, sibling_index, !left);
                    parent = self.node(index).links.parent;
                    sibling = parent.and_then(|parent| if left { self.node(parent).links.right } else { self.node(parent).links.left });
                }
                if let Some(sibling) = sibling {
                    let parent_red = parent.is_some_and(|parent| self.node(parent).links.red);
                    self.node_mut(sibling).links.red = parent_red;
                    let outer = if left { self.node(sibling).links.right } else { self.node(sibling).links.left };
                    if let Some(outer) = outer { self.node_mut(outer).links.red = false; }
                }
                if let Some(parent) = parent { self.node_mut(parent).links.red = false; root = self.rotate(root, parent, left); }
                current = Some(root);
            }
        }
    }
    fn removal_red(&self, index: Option<usize>) -> bool { index.is_some_and(|index| self.node(index).links.red) }
}
