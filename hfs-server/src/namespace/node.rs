use std::{fmt::Debug, ptr::NonNull};

use hfs_proto::master::FileMetaInfo;
use volo::FastStr;

use crate::{
    auth::{AuthManager, Permissions},
    util::{
        search::{binary_search, BinarySearch},
        time::current_timestamp,
    },
};

#[derive(Debug)]
pub struct Node {
    pub(super) node_id: i64,
    pub(super) name: Vec<u8>,
    pub(super) node_info: NodeInfo,
    pub(super) mdfc_time: u64,
    pub(super) acs_time: u64,
    pub(super) perms: Permissions,
}

pub enum NodeInfo {
    Directory(Vec<NonNull<Node>>),
    File(Vec<i64>), // chunk id
}

#[derive(Debug)]
pub enum NodeType {
    FileNode,
    DirectoryNode,
}

impl BinarySearch for NonNull<Node> {
    type Key = [u8];

    fn get_key(&self) -> &Self::Key {
        unsafe { &self.as_ref().name }
    }
}

impl Node {
    pub fn new(node_id: i64, name: Vec<u8>, ntype: NodeType) -> Node {
        let ct = current_timestamp();

        Node {
            node_id,
            name,
            node_info: match ntype {
                NodeType::FileNode => NodeInfo::File(vec![]),
                NodeType::DirectoryNode => NodeInfo::Directory(vec![]),
            },
            mdfc_time: ct,
            acs_time: ct,
            perms: Permissions::default(),
        }
    }

    pub fn with_info(node_id: i64, name: Vec<u8>, node_info: NodeInfo) -> Node {
        let ct = current_timestamp();

        Node {
            node_id,
            name,
            node_info,
            mdfc_time: ct,
            acs_time: ct,
            perms: Permissions::default(),
        }
    }

    pub fn node_id(&self) -> i64 {
        self.node_id
    }

    pub fn is_file(&self) -> bool {
        match self.node_info {
            NodeInfo::Directory(_) => false,
            NodeInfo::File(_) => true,
        }
    }

    pub fn is_directory(&self) -> bool {
        match self.node_info {
            NodeInfo::Directory(_) => true,
            NodeInfo::File(_) => false,
        }
    }

    // Some(Node) if the child with this name already exists;
    // otherwise, return None;
    pub fn add_child(&mut self, mut node: Box<Node>) -> Option<Box<Node>> {
        match &mut self.node_info {
            NodeInfo::Directory(child_node) => {
                let (is_exists, index) = binary_search(child_node, &node.name);
                if is_exists {
                    return Some(node);
                }
                if node.perms.is_unset() {
                    node.perms = self.perms;
                }

                child_node.insert(index, box_to_ptr(node));
                None
            }
            NodeInfo::File(_) => Some(node),
        }
    }

    pub fn remove_child(&mut self, name: &[u8]) -> Option<NonNull<Node>> {
        match &mut self.node_info {
            NodeInfo::Directory(child_node) => {
                if let (true, index) = binary_search(child_node, name) {
                    return Some(child_node.remove(index));
                }
                None
            }
            NodeInfo::File(_) => None,
        }
    }

    pub fn get_child(&self, name: &[u8]) -> Option<&Node> {
        if let NodeInfo::Directory(child_node) = &self.node_info {
            if let (true, index) = binary_search(child_node, name) {
                return Some(unsafe { child_node.get_unchecked(index).as_ref() });
            }
        }
        None
    }

    pub fn get_child_mut(&mut self, name: &[u8]) -> Option<&mut Node> {
        if let NodeInfo::Directory(child_node) = &mut self.node_info {
            if let (true, index) = binary_search(child_node, name) {
                return Some(unsafe { child_node.get_unchecked_mut(index).as_mut() });
            }
        }
        None
    }

    pub fn rename(&mut self, name: &[u8]) {
        self.name = name.to_vec();
    }

    pub fn get_child_ptr(&self, name: &[u8]) -> Option<NonNull<Node>> {
        if let NodeInfo::Directory(child_node) = &self.node_info {
            if let (true, index) = binary_search(child_node, name) {
                return Some(unsafe { *child_node.get_unchecked(index) });
            }
        }
        None
    }

    pub fn child_node(&self) -> Option<&Vec<NonNull<Node>>> {
        match &self.node_info {
            NodeInfo::Directory(child_node) => Some(child_node),
            NodeInfo::File(_) => None,
        }
    }

    pub fn child_node_mut(&mut self) -> Option<&mut Vec<NonNull<Node>>> {
        match &mut self.node_info {
            NodeInfo::Directory(child_node) => Some(child_node),
            NodeInfo::File(_) => None,
        }
    }

    pub fn set_perms(&mut self, perms: Permissions) {
        self.perms = perms
    }

    pub fn perms(&self) -> &Permissions {
        &self.perms
    }

    pub fn chunk_ids(&self) -> Option<&[i64]> {
        if let NodeInfo::File(chunks) = &self.node_info {
            return Some(chunks);
        }
        None
    }

    pub fn to_file_info(&self, auth_mgr: &AuthManager) -> Option<FileMetaInfo> {
        let username = auth_mgr.get_user_name(self.perms.owner_id)?;
        let group_name = auth_mgr.get_group_name(self.perms.group_id)?;
        Some(FileMetaInfo {
            src: FastStr::empty(),
            name: FastStr::new_u8_slice(&self.name).ok()?,
            len: 0,
            mdfc_time: self.mdfc_time,
            acs_time: self.acs_time,
            owner: FastStr::from_string(username),
            group: FastStr::from_string(group_name),
            perms: self.perms.op_perms.to_vec(),
            chunks: vec![],
            file_id: self.node_id,
        })
    }
}

pub fn box_to_ptr<T>(box_t: Box<T>) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(Box::into_raw(box_t)) }
}

impl Debug for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Directory(arg0) => f
                .debug_tuple("Directory")
                .field(
                    &arg0
                        .iter()
                        .map(|ptr| unsafe { ptr.as_ref() })
                        .collect::<Vec<_>>(),
                )
                .finish(),
            Self::File(arg0) => f.debug_tuple("File").field(arg0).finish(),
        }
    }
}
