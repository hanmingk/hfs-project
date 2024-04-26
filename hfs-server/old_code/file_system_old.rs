use std::sync::{
    atomic::{AtomicI64, Ordering},
    Mutex,
};

use super::{
    node::{Node, NodeID},
    path::HFSPath,
};

pub const NODE_LEASE_PREFIX: [u8; 4] = *b"node";

#[derive(Debug)]
pub struct FileSystem {
    pub(super) fs_directory: Mutex<FSDirectory>,
    pub(super) atomic: AtomicI64,
}

#[derive(Debug)]
pub struct FSDirectory {
    super_root: Node,
    pub count: usize,
}

impl FileSystem {
    pub fn new() -> FileSystem {
        FileSystem {
            fs_directory: Mutex::new(FSDirectory::new()),
            atomic: AtomicI64::new(2),
        }
    }

    pub fn next_node_id(&self) -> NodeID {
        let id = self.atomic.fetch_add(1, Ordering::Relaxed);
        if id < 0 {
            panic!("Over `i64::MAX` node id created.");
        }
        NodeID(id)
    }
}

impl FSDirectory {
    pub fn new() -> FSDirectory {
        FSDirectory {
            super_root: Node::with_root(),
            count: 2,
        }
    }

    pub fn contains_path(&self, src: &HFSPath) -> bool {
        if let Some(_) = self.get_node(src) {
            return true;
        }
        false
    }

    pub fn get_node(&self, src: &HFSPath) -> Option<&Node> {
        let mut path_components = src.components();
        let mut crt_node = &self.super_root;
        while let Some(name) = path_components.next() {
            if let Some(child_node) = crt_node.get_child(name) {
                crt_node = child_node;
            } else {
                return None;
            }
        }

        Some(crt_node)
    }

    pub fn get_node_mut(&mut self, src: &HFSPath) -> Option<&mut Node> {
        let mut path_components = src.components();
        let mut crt_node = &mut self.super_root;
        while let Some(name) = path_components.next() {
            if let Some(child_node) = crt_node.get_child_mut(name) {
                crt_node = child_node;
            } else {
                return None;
            }
        }

        Some(crt_node)
    }
}
