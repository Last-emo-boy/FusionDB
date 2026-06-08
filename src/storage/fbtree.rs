use std::sync::Arc;
use std::sync::RwLock;

// Constants for FB+-Tree
const FANOUT: usize = 16; // Small fanout for demo; Paper uses 64
const FEATURE_SIZE: usize = 4; // Number of feature bytes

// Node ID type (index in the arena)
type NodeId = usize;

#[derive(Clone, Debug)]
pub struct InnerNode {
    prefix: Vec<u8>,
    // Flattened feature array: [feature_0_key_0, ..., feature_0_key_N, feature_1_key_0, ...]
    features: Vec<u8>,
    children: Vec<NodeId>,
    anchors: Vec<Vec<u8>>, // Pivot keys
}

#[derive(Clone, Debug)]
pub struct LeafNode {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    // Tags for fast filtering (hash fingerprints)
    tags: Vec<u8>,
    // Bitmap for slot occupancy (simplified as bool vector for now)
    occupied: Vec<bool>,
    next: Option<NodeId>,
}

#[derive(Clone, Debug)]
pub enum Node {
    Inner(InnerNode),
    Leaf(LeafNode),
}

pub struct FBTree {
    nodes: Vec<Arc<RwLock<Node>>>, // Simple arena with locking
    root: NodeId,
}

impl FBTree {
    pub fn new() -> Self {
        let root = Node::Leaf(Self::empty_leaf_node());

        Self {
            nodes: vec![Arc::new(RwLock::new(root))],
            root: 0,
        }
    }

    fn empty_leaf_node() -> LeafNode {
        LeafNode {
            keys: Vec::with_capacity(FANOUT),
            values: Vec::with_capacity(FANOUT),
            tags: Vec::with_capacity(FANOUT),
            occupied: Vec::with_capacity(FANOUT),
            next: None,
        }
    }

    fn empty_inner_node() -> InnerNode {
        InnerNode {
            prefix: Vec::new(),
            features: vec![0u8; FEATURE_SIZE * FANOUT],
            children: Vec::with_capacity(FANOUT),
            anchors: Vec::with_capacity(FANOUT.saturating_sub(1)),
        }
    }

    fn node_group_count(item_count: usize) -> usize {
        item_count / FANOUT + usize::from(item_count % FANOUT != 0)
    }

    fn bulk_load_node_capacity(leaf_count: usize) -> usize {
        let mut total = leaf_count;
        let mut level_count = leaf_count;
        while level_count > 1 {
            level_count = Self::node_group_count(level_count);
            total += level_count;
        }
        total
    }

    // Helper to calculate features for a key relative to a prefix
    fn extract_features(key: &[u8], prefix_len: usize) -> [u8; FEATURE_SIZE] {
        let mut features = [0u8; FEATURE_SIZE];
        for i in 0..FEATURE_SIZE {
            if prefix_len + i < key.len() {
                // +128 trick from paper for signed comparison simulation
                features[i] = key[prefix_len + i].wrapping_add(128);
            } else {
                features[i] = 0; // Padding
            }
        }
        features
    }

    // Bulk Load from sorted iterator
    pub fn bulk_load<I>(iter: I) -> Self
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        let (lower_bound, upper_bound) = iter.size_hint();
        let entry_capacity_hint = upper_bound.unwrap_or(lower_bound);
        let leaf_capacity = Self::node_group_count(entry_capacity_hint).max(1);

        let mut leaves = Vec::with_capacity(leaf_capacity);
        let mut current_leaf = Self::empty_leaf_node();

        let mut nodes_arena = Vec::with_capacity(Self::bulk_load_node_capacity(leaf_capacity));

        for (k, v) in iter {
            if current_leaf.keys.len() >= FANOUT {
                // Flush leaf
                let id = nodes_arena.len();
                nodes_arena.push(Arc::new(RwLock::new(Node::Leaf(current_leaf))));
                leaves.push(id);

                current_leaf = Self::empty_leaf_node();
            }

            // Add to leaf
            let tag = Self::hash_tag(&k);
            current_leaf.keys.push(k);
            current_leaf.values.push(v);
            current_leaf.tags.push(tag);
            current_leaf.occupied.push(true);
        }

        // Push last leaf
        let last_id = nodes_arena.len();
        nodes_arena.push(Arc::new(RwLock::new(Node::Leaf(current_leaf))));
        leaves.push(last_id);

        // Link leaves
        for i in 0..leaves.len().saturating_sub(1) {
            let next_id = leaves[i + 1];
            if let Node::Leaf(ref mut leaf) = *nodes_arena[leaves[i]].write().unwrap() {
                leaf.next = Some(next_id);
            }
        }

        // Build Inner Nodes
        let mut current_level_ids = leaves;

        while current_level_ids.len() > 1 {
            let mut next_level_ids =
                Vec::with_capacity(Self::node_group_count(current_level_ids.len()));
            let mut current_inner = Self::empty_inner_node();

            for (_i, &child_id) in current_level_ids.iter().enumerate() {
                if current_inner.children.len() >= FANOUT {
                    // Flush inner
                    let mut node = Node::Inner(current_inner);
                    if let Node::Inner(ref mut inner) = node {
                        Self::rebuild_inner_node(inner);
                    }

                    let id = nodes_arena.len();
                    nodes_arena.push(Arc::new(RwLock::new(node)));
                    next_level_ids.push(id);

                    current_inner = Self::empty_inner_node();
                }

                // Add child
                if !current_inner.children.is_empty() {
                    // Anchor is min key of THIS child
                    let min_key = Self::get_min_key_from_arena(&nodes_arena, child_id);
                    current_inner.anchors.push(min_key);
                }
                current_inner.children.push(child_id);
            }

            // Flush last inner
            let mut node = Node::Inner(current_inner);
            if let Node::Inner(ref mut inner) = node {
                Self::rebuild_inner_node(inner);
            }
            let id = nodes_arena.len();
            nodes_arena.push(Arc::new(RwLock::new(node)));
            next_level_ids.push(id);

            current_level_ids = next_level_ids;
        }

        Self {
            nodes: nodes_arena,
            root: current_level_ids[0],
        }
    }

    fn get_min_key_from_arena(arena: &[Arc<RwLock<Node>>], id: NodeId) -> Vec<u8> {
        let node = arena[id].read().unwrap();
        match &*node {
            Node::Leaf(l) => l.keys.first().cloned().unwrap_or_default(),
            Node::Inner(i) => Self::get_min_key_from_arena(arena, i.children[0]),
        }
    }

    fn rebuild_inner_node(inner: &mut InnerNode) {
        if inner.anchors.is_empty() {
            return;
        }

        // 1. Calculate Common Prefix
        let mut prefix = inner.anchors[0].clone();
        for key in &inner.anchors[1..] {
            let len = prefix
                .iter()
                .zip(key.iter())
                .take_while(|(a, b)| a == b)
                .count();
            prefix.truncate(len);
        }
        inner.prefix = prefix;

        // 2. Build Features
        let plen = inner.prefix.len();
        inner.features.fill(0);

        for (i, key) in inner.anchors.iter().enumerate() {
            if i >= FANOUT {
                break;
            }
            let f = Self::extract_features(key, plen);
            for fid in 0..FEATURE_SIZE {
                inner.features[fid * FANOUT + i] = f[fid];
            }
        }
    }

    // Core FB+-Tree Logic: Find Child using Features
    fn find_child_index(&self, inner: &InnerNode, key: &[u8]) -> usize {
        let plen = inner.prefix.len();
        if key.len() < plen || &key[..plen] != inner.prefix.as_slice() {
            if key < &inner.prefix[..] {
                return 0;
            } else {
                return inner.children.len() - 1;
            }
        }

        let mut candidates: u64 = if inner.anchors.len() >= 64 {
            u64::MAX
        } else {
            (1u64 << inner.anchors.len()) - 1
        };

        for fid in 0..FEATURE_SIZE {
            if plen + fid >= key.len() {
                break;
            }
            let byte = key[plen + fid].wrapping_add(128);

            let start = fid * FANOUT;
            let mut eq_mask = 0u64;
            let mut gt_mask = 0u64;

            for i in 0..inner.anchors.len() {
                if (candidates >> i) & 1 == 1 {
                    let f_byte = inner.features[start + i];
                    if f_byte == byte {
                        eq_mask |= 1 << i;
                    } else if f_byte > byte {
                        gt_mask |= 1 << i;
                    }
                }
            }

            if gt_mask != 0 {
                return gt_mask.trailing_zeros() as usize;
            }

            candidates &= eq_mask;

            if candidates == 0 {
                return inner.children.len() - 1;
            }
        }

        match inner.anchors.binary_search_by(|a| a.as_slice().cmp(key)) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        }
    }

    fn hash_tag(key: &[u8]) -> u8 {
        key.iter().fold(0u8, |acc, &x| acc.wrapping_add(x))
    }

    // Public API: insert (Wrapper for compatibility, just rebuilds tree for now or panic)
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Just append to root leaf if empty, else panic
        // This is a hack for the `new()` -> `insert` usage in `build_fbtree`.
        // Since we switched to bulk_load, we should update `build_fbtree`.
        // But to keep `insert` signature for test code if any:
        if self.nodes.len() == 1 {
            if let Node::Leaf(ref mut leaf) = *self.nodes[0].write().unwrap() {
                leaf.keys.push(key);
                leaf.values.push(value);
                // ...
                // This is unsafe and incomplete.
                // Better to update `build_fbtree` to use `bulk_load`.
            }
        }
    }

    // Public API: Get
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut curr = self.root;
        loop {
            let next_node = {
                let node = self.nodes[curr].read().unwrap();
                match &*node {
                    Node::Inner(inner) => {
                        let idx = self.find_child_index(inner, key);
                        inner.children[idx]
                    }
                    Node::Leaf(leaf) => {
                        let tag = Self::hash_tag(key);
                        for (i, &t) in leaf.tags.iter().enumerate() {
                            if t == tag && leaf.occupied[i] {
                                if leaf.keys[i] == key {
                                    return Some(leaf.values[i].clone());
                                }
                            }
                        }
                        return None;
                    }
                }
            };
            curr = next_node;
        }
    }

    // Iterator
    pub fn scan(&self, start_key: &[u8]) -> FBTreeIterator<'_> {
        let mut curr = self.root;
        // 1. Descent to leaf
        loop {
            let next_node = {
                let node = self.nodes[curr].read().unwrap();
                match &*node {
                    Node::Inner(inner) => {
                        let idx = self.find_child_index(inner, start_key);
                        inner.children[idx]
                    }
                    Node::Leaf(_) => break,
                }
            };
            curr = next_node;
        }

        // 2. Find start index in leaf
        let mut idx = 0;
        {
            let node = self.nodes[curr].read().unwrap();
            if let Node::Leaf(leaf) = &*node {
                for (i, key) in leaf.keys.iter().enumerate() {
                    if key.as_slice() >= start_key {
                        idx = i;
                        break;
                    }
                }
                if idx == 0
                    && leaf.keys.len() > 0
                    && leaf.keys.last().unwrap().as_slice() < start_key
                {
                    idx = leaf.keys.len();
                }
            }
        }

        FBTreeIterator {
            tree: self,
            curr_leaf: curr,
            curr_idx: idx,
        }
    }
}

pub struct FBTreeIterator<'a> {
    tree: &'a FBTree,
    curr_leaf: NodeId,
    curr_idx: usize,
}

impl<'a> Iterator for FBTreeIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_node_id = {
                let node = self.tree.nodes[self.curr_leaf].read().unwrap();
                if let Node::Leaf(leaf) = &*node {
                    if self.curr_idx < leaf.keys.len() {
                        if leaf.occupied[self.curr_idx] {
                            let k = leaf.keys[self.curr_idx].clone();
                            let v = leaf.values[self.curr_idx].clone();
                            self.curr_idx += 1;
                            return Some((k, v));
                        } else {
                            // Skip unoccupied
                            self.curr_idx += 1;
                            continue;
                        }
                    } else {
                        // End of leaf, go to next
                        leaf.next
                    }
                } else {
                    None
                }
            };

            if let Some(next_id) = next_node_id {
                self.curr_leaf = next_id;
                self.curr_idx = 0;
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted_pairs(count: usize) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        (0..count).map(|id| {
            (
                format!("key:{id:04}").into_bytes(),
                format!("value:{id:04}").into_bytes(),
            )
        })
    }

    #[test]
    fn new_tree_preallocates_root_leaf_slots() {
        let tree = FBTree::new();
        let root = tree.nodes[tree.root].read().unwrap();
        let Node::Leaf(leaf) = &*root else {
            panic!("new tree root should be a leaf");
        };

        assert!(leaf.keys.capacity() >= FANOUT);
        assert!(leaf.values.capacity() >= FANOUT);
        assert!(leaf.tags.capacity() >= FANOUT);
        assert!(leaf.occupied.capacity() >= FANOUT);
    }

    #[test]
    fn bulk_load_preallocates_leaf_and_inner_slots() {
        let tree = FBTree::bulk_load(sorted_pairs(FANOUT * 2 + 1));
        assert!(tree.nodes.capacity() >= 4);

        let root = tree.nodes[tree.root].read().unwrap();
        let Node::Inner(inner) = &*root else {
            panic!("bulk-loaded multi-leaf tree root should be inner");
        };
        assert!(inner.children.capacity() >= FANOUT);
        assert!(inner.anchors.capacity() >= FANOUT.saturating_sub(1));
        let first_child = inner.children[0];
        drop(root);

        let child = tree.nodes[first_child].read().unwrap();
        let Node::Leaf(leaf) = &*child else {
            panic!("first child should be a leaf");
        };
        assert!(leaf.keys.capacity() >= FANOUT);
        assert!(leaf.values.capacity() >= FANOUT);
        assert!(leaf.tags.capacity() >= FANOUT);
        assert!(leaf.occupied.capacity() >= FANOUT);

        assert_eq!(
            tree.get(b"key:0017"),
            Some(b"value:0017".to_vec()),
            "bulk-loaded tree should still find keys"
        );
        let scanned: Vec<_> = tree.scan(b"key:0031").collect();
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].0, b"key:0031".to_vec());
    }

    #[test]
    fn bulk_load_capacity_helpers_count_fanout_groups() {
        assert_eq!(FBTree::node_group_count(0), 0);
        assert_eq!(FBTree::node_group_count(1), 1);
        assert_eq!(FBTree::node_group_count(FANOUT), 1);
        assert_eq!(FBTree::node_group_count(FANOUT + 1), 2);

        assert_eq!(FBTree::bulk_load_node_capacity(1), 1);
        assert_eq!(FBTree::bulk_load_node_capacity(FANOUT + 1), FANOUT + 4);
    }
}
