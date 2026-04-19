use std::cmp::Ordering;

use crate::dal::{PageId, MAX_PAGES};

pub const MAX_KEY_LEN: usize = 32;
pub const MAX_NODES: usize = MAX_PAGES;

/// Sentinel index meaning "no child / empty".
const NULL: usize = usize::MAX;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum BstError {
    /// The tree has no free node slots.
    Full,
    /// The requested key does not exist in the tree.
    NotFound,
    /// Key length is 0 or exceeds MAX_KEY_LEN.
    InvalidKey,
}

impl std::fmt::Display for BstError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BstError::Full => write!(f, "BST is full (max {} nodes)", MAX_NODES),
            BstError::NotFound => write!(f, "key not found"),
            BstError::InvalidKey => {
                write!(f, "key must be between 1 and {} bytes", MAX_KEY_LEN)
            }
        }
    }
}

impl std::error::Error for BstError {}

// ---------------------------------------------------------------------------
// Internal node stored in the arena
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Node {
    key: [u8; MAX_KEY_LEN],
    key_len: usize,
    value: PageId,
    left: usize,  // index into arena, or NULL
    right: usize, // index into arena, or NULL
}

impl Default for Node {
    fn default() -> Self {
        Node {
            key: [0u8; MAX_KEY_LEN],
            key_len: 0,
            value: 0,
            left: NULL,
            right: NULL,
        }
    }
}

// ---------------------------------------------------------------------------
// BST
// ---------------------------------------------------------------------------

/// Fixed-capacity binary search tree backed by a flat arena.
///
/// Keys are byte strings of length 1..=MAX_KEY_LEN.  Values are PageIds.
/// Memory is entirely stack-allocated; no heap is used.
pub struct Bst {
    nodes: [Node; MAX_NODES],
    root: usize,                 // NULL when the tree is empty
    free_list: [usize; MAX_NODES], // stack of free arena indices
    free_count: usize,
}

impl Bst {
    pub fn new() -> Self {
        let mut free_list = [0usize; MAX_NODES];
        for i in 0..MAX_NODES {
            free_list[i] = i;
        }
        Bst {
            nodes: [Node::default(); MAX_NODES],
            root: NULL,
            free_list,
            free_count: MAX_NODES,
        }
    }

    /// Number of entries currently stored.
    pub fn len(&self) -> usize {
        MAX_NODES - self.free_count
    }

    pub fn is_empty(&self) -> bool {
        self.root == NULL
    }

    // -----------------------------------------------------------------------
    // Public operations
    // -----------------------------------------------------------------------

    /// Return the PageId associated with `key`, or `BstError::NotFound`.
    pub fn get(&self, key: &[u8]) -> Result<PageId, BstError> {
        Self::validate_key(key)?;
        let mut curr = self.root;
        while curr != NULL {
            let node = &self.nodes[curr];
            match key.cmp(&node.key[..node.key_len]) {
                Ordering::Equal => return Ok(node.value),
                Ordering::Less => curr = node.left,
                Ordering::Greater => curr = node.right,
            }
        }
        Err(BstError::NotFound)
    }

    /// Insert `(key, value)`.  If `key` already exists its value is overwritten.
    /// Returns `BstError::Full` when the arena is exhausted.
    pub fn insert(&mut self, key: &[u8], value: PageId) -> Result<(), BstError> {
        Self::validate_key(key)?;

        let mut curr = self.root;
        let mut parent = NULL;
        let mut is_left_child = false;

        while curr != NULL {
            // Copy fields out so the immutable borrow ends before any mutation.
            let (ord, left, right) = {
                let node = &self.nodes[curr];
                (key.cmp(&node.key[..node.key_len]), node.left, node.right)
            };
            match ord {
                Ordering::Equal => {
                    self.nodes[curr].value = value;
                    return Ok(());
                }
                Ordering::Less => {
                    parent = curr;
                    is_left_child = true;
                    curr = left;
                }
                Ordering::Greater => {
                    parent = curr;
                    is_left_child = false;
                    curr = right;
                }
            }
        }

        // Allocate a free slot.
        if self.free_count == 0 {
            return Err(BstError::Full);
        }
        self.free_count -= 1;
        let new_idx = self.free_list[self.free_count];

        let mut new_key = [0u8; MAX_KEY_LEN];
        new_key[..key.len()].copy_from_slice(key);
        self.nodes[new_idx] = Node {
            key: new_key,
            key_len: key.len(),
            value,
            left: NULL,
            right: NULL,
        };

        if parent == NULL {
            self.root = new_idx;
        } else if is_left_child {
            self.nodes[parent].left = new_idx;
        } else {
            self.nodes[parent].right = new_idx;
        }

        Ok(())
    }

    /// Remove the entry for `key`.  Returns `BstError::NotFound` if absent.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), BstError> {
        Self::validate_key(key)?;

        // Locate the node and remember its parent.
        let mut parent = NULL;
        let mut is_left_child = false;
        let mut curr = self.root;

        while curr != NULL {
            let (ord, left, right) = {
                let node = &self.nodes[curr];
                (key.cmp(&node.key[..node.key_len]), node.left, node.right)
            };
            match ord {
                Ordering::Equal => break,
                Ordering::Less => {
                    parent = curr;
                    is_left_child = true;
                    curr = left;
                }
                Ordering::Greater => {
                    parent = curr;
                    is_left_child = false;
                    curr = right;
                }
            }
        }

        if curr == NULL {
            return Err(BstError::NotFound);
        }

        let left = self.nodes[curr].left;
        let right = self.nodes[curr].right;

        if left != NULL && right != NULL {
            // Two children: replace with in-order successor (leftmost in right subtree).
            let mut succ_parent = curr;
            let mut succ = right;
            while self.nodes[succ].left != NULL {
                succ_parent = succ;
                succ = self.nodes[succ].left;
            }

            // Copy the successor's data (all Copy types) before mutating.
            let succ_key = self.nodes[succ].key;
            let succ_key_len = self.nodes[succ].key_len;
            let succ_value = self.nodes[succ].value;
            let succ_right = self.nodes[succ].right;

            self.nodes[curr].key = succ_key;
            self.nodes[curr].key_len = succ_key_len;
            self.nodes[curr].value = succ_value;

            // Detach successor (it has at most a right child).
            if succ_parent == curr {
                self.nodes[succ_parent].right = succ_right;
            } else {
                self.nodes[succ_parent].left = succ_right;
            }
            self.free_node(succ);
        } else {
            // Zero or one child.
            let replacement = if left != NULL { left } else { right }; // NULL when leaf
            if parent == NULL {
                self.root = replacement;
            } else if is_left_child {
                self.nodes[parent].left = replacement;
            } else {
                self.nodes[parent].right = replacement;
            }
            self.free_node(curr);
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn validate_key(key: &[u8]) -> Result<(), BstError> {
        if key.is_empty() || key.len() > MAX_KEY_LEN {
            Err(BstError::InvalidKey)
        } else {
            Ok(())
        }
    }

    fn free_node(&mut self, idx: usize) {
        self.nodes[idx] = Node::default();
        self.free_list[self.free_count] = idx;
        self.free_count += 1;
    }
}

impl Default for Bst {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut bst = Bst::new();
        bst.insert(b"hello", 42).unwrap();
        assert_eq!(bst.get(b"hello"), Ok(42));
        assert_eq!(bst.len(), 1);
    }

    #[test]
    fn upsert_overwrites() {
        let mut bst = Bst::new();
        bst.insert(b"key", 1).unwrap();
        bst.insert(b"key", 2).unwrap();
        assert_eq!(bst.get(b"key"), Ok(2));
        assert_eq!(bst.len(), 1);
    }

    #[test]
    fn get_missing_returns_not_found() {
        let bst = Bst::new();
        assert_eq!(bst.get(b"missing"), Err(BstError::NotFound));
    }

    #[test]
    fn delete_leaf() {
        let mut bst = Bst::new();
        bst.insert(b"b", 10).unwrap();
        bst.insert(b"a", 20).unwrap();
        bst.insert(b"c", 30).unwrap();
        bst.delete(b"a").unwrap();
        assert_eq!(bst.get(b"a"), Err(BstError::NotFound));
        assert_eq!(bst.get(b"b"), Ok(10));
        assert_eq!(bst.get(b"c"), Ok(30));
        assert_eq!(bst.len(), 2);
    }

    #[test]
    fn delete_one_child() {
        let mut bst = Bst::new();
        bst.insert(b"b", 10).unwrap();
        bst.insert(b"a", 20).unwrap();
        bst.delete(b"b").unwrap();
        assert_eq!(bst.get(b"b"), Err(BstError::NotFound));
        assert_eq!(bst.get(b"a"), Ok(20));
        assert_eq!(bst.len(), 1);
    }

    #[test]
    fn delete_two_children() {
        let mut bst = Bst::new();
        bst.insert(b"d", 4).unwrap();
        bst.insert(b"b", 2).unwrap();
        bst.insert(b"f", 6).unwrap();
        bst.insert(b"e", 5).unwrap();
        bst.insert(b"g", 7).unwrap();
        bst.delete(b"f").unwrap();
        assert_eq!(bst.get(b"f"), Err(BstError::NotFound));
        assert_eq!(bst.get(b"e"), Ok(5));
        assert_eq!(bst.get(b"g"), Ok(7));
        assert_eq!(bst.len(), 4);
    }

    #[test]
    fn delete_root() {
        let mut bst = Bst::new();
        bst.insert(b"root", 99).unwrap();
        bst.delete(b"root").unwrap();
        assert!(bst.is_empty());
    }

    #[test]
    fn delete_missing_returns_not_found() {
        let mut bst = Bst::new();
        assert_eq!(bst.delete(b"ghost"), Err(BstError::NotFound));
    }

    #[test]
    fn freed_slots_are_reused() {
        let mut bst = Bst::new();
        bst.insert(b"x", 1).unwrap();
        bst.delete(b"x").unwrap();
        bst.insert(b"y", 2).unwrap();
        assert_eq!(bst.len(), 1);
    }

    #[test]
    fn full_returns_error() {
        let mut bst = Bst::new();
        for i in 0..MAX_NODES {
            // Use a unique 1-byte key by cycling through printable ASCII.
            // Since MAX_NODES (256) > 95 printable chars, use multi-byte keys.
            let key = format!("k{:03}", i);
            bst.insert(key.as_bytes(), i).unwrap();
        }
        assert_eq!(bst.insert(b"overflow", 0), Err(BstError::Full));
    }

    #[test]
    fn invalid_key_empty() {
        let mut bst = Bst::new();
        assert_eq!(bst.insert(b"", 1), Err(BstError::InvalidKey));
    }

    #[test]
    fn invalid_key_too_long() {
        let mut bst = Bst::new();
        let key = [b'x'; MAX_KEY_LEN + 1];
        assert_eq!(bst.insert(&key, 1), Err(BstError::InvalidKey));
    }

    #[test]
    fn max_length_key_accepted() {
        let mut bst = Bst::new();
        let key = [b'z'; MAX_KEY_LEN];
        bst.insert(&key, 7).unwrap();
        assert_eq!(bst.get(&key), Ok(7));
    }
}
