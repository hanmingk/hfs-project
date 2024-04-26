use crate::{auth::PermissionsInfo, chunk::ChunkID, util::time::current_timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeID(pub(super) i64);

#[derive(Debug)]
pub struct Node {
    pub(super) id: NodeID,
    pub(super) name: Vec<u8>,
    pub(super) node_info: NodeInfo,
    pub(super) mdfc_time: u64,
    pub(super) acs_time: u64,
    pub(super) permissions_info: PermissionsInfo,
}

#[derive(Debug)]
pub enum NodeType {
    Directory,
    File,
}

impl From<NodeType> for NodeInfo {
    fn from(value: NodeType) -> Self {
        match value {
            NodeType::Directory => NodeInfo::Directory(Default::default()),
            NodeType::File => NodeInfo::File(Default::default()),
        }
    }
}

#[derive(Debug)]
pub enum NodeInfo {
    Directory(DirectoryInfo),
    File(FileInfo),
}

#[derive(Debug, Default)]
pub struct FileInfo {
    pub chunks: Vec<ChunkID>,
}

#[derive(Debug, Default)]
struct DirectoryInfo {
    floder: Vec<Node>,
}

impl Node {
    pub fn with_root() -> Node {
        let mut super_root = Node::new(NodeID(0), vec![], NodeType::Directory);
        let root_permissions_info = PermissionsInfo::with_id(0, 0);
        let mut root = Node::with_permissions(
            NodeID(0),
            vec![],
            NodeType::Directory,
            root_permissions_info,
        );
        let home_dir = Node::with_permissions(
            NodeID(1),
            b"home".to_vec(),
            NodeType::Directory,
            root_permissions_info,
        );
        root.add_child(home_dir);
        super_root.add_child(root);
        return super_root;
    }

    pub fn new(id: NodeID, name: Vec<u8>, node_type: NodeType) -> Node {
        let ct = current_timestamp();
        Node {
            id,
            name,
            node_info: node_type.into(),
            mdfc_time: ct,
            acs_time: ct,
            permissions_info: PermissionsInfo::default(),
        }
    }

    pub fn with_permissions(
        id: NodeID,
        name: Vec<u8>,
        node_type: NodeType,
        permissions_info: PermissionsInfo,
    ) -> Node {
        let mut node = Node::new(id, name, node_type);
        node.permissions_info = permissions_info;
        node
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

    pub fn set_permissions_info(&mut self, permissions_info: PermissionsInfo) {
        self.permissions_info = permissions_info;
    }

    pub fn contains_name(&self, name: &[u8]) -> bool {
        match self.search_childern(name) {
            Some(_) => true,
            None => false,
        }
    }

    // Some(Node) if the child with this name already exists;
    // otherwise, return None;
    pub fn add_child(&mut self, mut node: Node) -> Option<Node> {
        match &mut self.node_info {
            NodeInfo::Directory(dir_info) => {
                if node.permissions_info.is_unset() {
                    node.permissions_info = self.permissions_info;
                }
                dir_info.add_child(node)
            }
            NodeInfo::File(_) => Some(node),
        }
    }

    pub fn remove_child(&mut self, name: &[u8]) -> Option<Node> {
        match &mut self.node_info {
            NodeInfo::Directory(dir_info) => dir_info.remove_child(name),
            NodeInfo::File(_) => None,
        }
    }

    pub fn get_child(&self, name: &[u8]) -> Option<&Node> {
        if let NodeInfo::Directory(dir_info) = &self.node_info {
            if let (true, index) = dir_info.search_childern(name) {
                return Some(unsafe { dir_info.floder.get_unchecked(index) });
            }
        }
        None
    }

    pub fn get_child_mut(&mut self, name: &[u8]) -> Option<&mut Node> {
        if let NodeInfo::Directory(dir_info) = &mut self.node_info {
            if let (true, index) = dir_info.search_childern(name) {
                return Some(unsafe { dir_info.floder.get_unchecked_mut(index) });
            }
        }
        None
    }

    fn search_childern(&self, name: &[u8]) -> Option<usize> {
        if let NodeInfo::Directory(dir_info) = &self.node_info {
            if let (true, index) = dir_info.search_childern(name) {
                return Some(index);
            }
        }
        None
    }
}

impl DirectoryInfo {
    fn add_child(&mut self, node: Node) -> Option<Node> {
        let (is_exists, index) = self.search_childern(&node.name);
        if is_exists {
            return Some(node);
        }
        self.floder.insert(index, node);
        None
    }

    fn remove_child(&mut self, name: &[u8]) -> Option<Node> {
        let (is_exists, index) = self.search_childern(name);
        if !is_exists {
            return None;
        }
        Some(self.floder.remove(index))
    }

    // (true, index) if the child with this name already exists;
    // otherwise, return (false, index) that index is insertion point;
    fn search_childern(&self, name: &[u8]) -> (bool, usize) {
        if self.floder.is_empty() {
            return (false, 0);
        }

        // Binary search
        let mut low = 0;
        let mut high = self.floder.len() - 1;

        while low <= high {
            let mid = (low + high) >> 1;
            match unsafe { name.cmp(&self.floder.get_unchecked(mid).name) } {
                std::cmp::Ordering::Less => high = mid - 1,
                std::cmp::Ordering::Equal => return (true, mid),
                std::cmp::Ordering::Greater => low = mid + 1,
            }
        }
        (false, low)
    }
}
