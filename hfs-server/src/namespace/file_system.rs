use std::{collections::VecDeque, fmt::Debug, marker::PhantomData, ptr::NonNull, sync::Mutex};

use ahash::AHashMap;
use hfs_proto::master::{list_directory_response::ListItem, FileMetaInfo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use volo::FastStr;

use crate::{
    auth::{AuthManager, OPPermissions, Permissions},
    chunk::masterserver::ChunkManager,
    cluster::ClusterWatcher,
    namespace::node::NodeInfo,
    util::gen_id::GenIdI64,
};

use super::{
    error::HFSIoError,
    node::{box_to_ptr, Node, NodeType},
    path::HFSPath,
};

#[derive(Debug)]
pub struct FileSystem {
    open_cache: Mutex<AHashMap<i64, NonNull<Node>>>,
    fs_directory: Mutex<FSDirectory>,
    gen_id: GenIdI64,
}

pub struct FSDirectory {
    super_root: NonNull<Node>,
    count: usize,
    marker: PhantomData<Box<Node>>,
}

impl FileSystem {
    pub fn new() -> FileSystem {
        FileSystem {
            open_cache: Mutex::new(AHashMap::new()),
            fs_directory: Mutex::new(FSDirectory::init()),
            gen_id: GenIdI64::new(1),
        }
    }

    pub fn from(fd: FSDirectory, atomic_id: i64) -> FileSystem {
        FileSystem {
            open_cache: Mutex::new(AHashMap::new()),
            fs_directory: Mutex::new(fd),
            gen_id: GenIdI64::new(atomic_id),
        }
    }

    pub fn create_file_or_directory(
        &self,
        auth_token: &str,
        auth_mgr: &AuthManager,
        path: HFSPath,
        ntype: NodeType,
        perms: Option<Permissions>,
    ) -> Result<(), HFSIoError> {
        let (user_id, _) = auth_mgr
            .verify_identity(auth_token)
            .ok_or(HFSIoError::UnSignIn)?;

        let parent_path = path
            .parent_path()
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

        let mut fs_dir = self.fs_directory.lock().unwrap();

        let parent_node = fs_dir
            .get_node_mut(&parent_path)
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

        match auth_mgr.verify_permissions(user_id, parent_node.perms()) {
            OPPermissions::WriteOnly | OPPermissions::ReadWithWrite => {
                let node_id = self.next_node_id();
                let node_name = path
                    .last_path_name()
                    .ok_or(HFSIoError::NoSuchFileOrDirectory)?;
                let mut new_node = Box::new(Node::new(node_id, node_name.to_vec(), ntype));
                if let Some(perms) = perms {
                    new_node.set_perms(perms);
                }
                if let Some(_) = parent_node.add_child(new_node) {
                    return Err(HFSIoError::ExistsFileOrDirectory);
                }
                fs_dir.count += 1;
                Ok(())
            }
            OPPermissions::NoPermissions | OPPermissions::ReadOnly => {
                Err(HFSIoError::NoPermissions)
            }
        }
    }

    pub fn remove_file_or_directory(
        &self,
        auth_token: &str,
        auth_mgr: &AuthManager,
        path: HFSPath,
    ) -> Result<Vec<i64>, HFSIoError> {
        let (user_id, _) = auth_mgr
            .verify_identity(auth_token)
            .ok_or(HFSIoError::UnSignIn)?;

        let parent_path = path
            .parent_path()
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

        let mut fs_dir = self.fs_directory.lock().unwrap();
        let parent_node = fs_dir
            .get_node_mut(&parent_path)
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

        let del_node_name = path
            .last_path_name()
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

        match parent_node.get_child(del_node_name) {
            Some(node) => {
                let open_cache = self.open_cache.lock().unwrap();
                if open_cache.get(&node.node_id).is_some() {
                    return Err(HFSIoError::FileAlreadyOpen);
                }
            }
            None => return Err(HFSIoError::NoSuchFileOrDirectory),
        }

        match auth_mgr.verify_permissions(user_id, parent_node.perms()) {
            OPPermissions::WriteOnly | OPPermissions::ReadWithWrite => {
                if let Some(del_ptr) = parent_node.remove_child(del_node_name) {
                    let node_tree = FSDirectory::from_ptr(del_ptr);
                    fs_dir.count -= node_tree.count;
                    return Ok(node_tree.all_chunks());
                }
                Err(HFSIoError::NoSuchFileOrDirectory)
            }
            OPPermissions::NoPermissions | OPPermissions::ReadOnly => {
                Err(HFSIoError::NoPermissions)
            }
        }
    }

    pub fn get_file_info(
        &self,
        auth_mgr: &AuthManager,
        chunk_mgr: &ChunkManager,
        cluster_watcher: &ClusterWatcher,
        file_id: &i64,
    ) -> Option<FileMetaInfo> {
        let open_cache = self.open_cache.lock().unwrap();
        let Some(node_ptr) = open_cache.get(file_id) else {
            return None;
        };

        let file_node = unsafe { node_ptr.as_ref() };
        let chunk_ids = file_node.chunk_ids()?;

        let chunks = chunk_mgr.get_chunks(chunk_ids, cluster_watcher);
        if chunks.len() != chunk_ids.len() {
            return None;
        }

        let mut file_info = file_node.to_file_info(auth_mgr)?;
        file_info.len = chunks.iter().fold(0, |acc, c| acc + c.num_bytes as u64);
        file_info.chunks = chunks;
        Some(file_info)
    }

    pub fn load_file_to_cache(
        &self,
        auth_token: &str,
        auth_mgr: &AuthManager,
        path: HFSPath,
    ) -> Result<i64, HFSIoError> {
        let (user_id, _) = auth_mgr
            .verify_identity(auth_token)
            .ok_or(HFSIoError::UnSignIn)?;

        let fs_dir = self.fs_directory.lock().unwrap();
        let node_prt = fs_dir
            .get_node_ptr(&path)
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;
        let node_ref = unsafe { node_prt.as_ref() };

        if let OPPermissions::NoPermissions = auth_mgr.verify_permissions(user_id, node_ref.perms())
        {
            return Err(HFSIoError::NoPermissions);
        }

        let mut file_cache = self.open_cache.lock().unwrap();
        file_cache.insert(node_ref.node_id(), node_prt);
        Ok(node_ref.node_id())
    }

    pub fn list_directory(
        &self,
        auth_mgr: &AuthManager,
        path: HFSPath,
    ) -> Result<Vec<ListItem>, HFSIoError> {
        let fs_dir = self.fs_directory.lock().unwrap();

        let node = fs_dir
            .get_node(&path)
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

        match &node.node_info {
            NodeInfo::Directory(nodes) => {
                let list_items = nodes
                    .iter()
                    .map(|node_ptr| {
                        let child_node = unsafe { node_ptr.as_ref() };
                        let perms = child_node.perms();

                        ListItem {
                            perms: perms.op_perms.to_vec(),
                            owner: auth_mgr
                                .get_user_name(perms.owner_id)
                                .unwrap_or("other".to_string())
                                .into(),
                            group: auth_mgr
                                .get_group_name(perms.group_id)
                                .unwrap_or("other".to_string())
                                .into(),
                            update_time: child_node.mdfc_time,
                            name: FastStr::from_vec_u8(child_node.name.clone())
                                .unwrap_or(FastStr::empty()),
                            is_dir: child_node.is_directory(),
                        }
                    })
                    .collect::<Vec<ListItem>>();

                Ok(list_items)
            }
            NodeInfo::File(_) => Err(HFSIoError::NoSuchFileOrDirectory),
        }
    }

    pub fn contains_path(&self, path: &HFSPath) -> Result<bool, HFSIoError> {
        let fs_dir = self.fs_directory.lock().unwrap();

        fs_dir
            .get_node(&path)
            .map(|n| n.is_directory())
            .ok_or(HFSIoError::NoSuchFileOrDirectory)
    }

    pub fn move_path(
        &self,
        auth_token: &str,
        auth_mgr: &AuthManager,
        src_path: HFSPath,
        target_path: HFSPath,
    ) -> Result<(), HFSIoError> {
        if src_path.is_root() {
            return Err(HFSIoError::NoPermissions);
        }

        let (user_id, _) = auth_mgr
            .verify_identity(auth_token)
            .ok_or(HFSIoError::UnSignIn)?;

        let src_parent = src_path
            .parent_path()
            .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

        let mut fs_dir = self.fs_directory.lock().unwrap();

        match fs_dir.get_node_ptr(&target_path) {
            Some(mut target_node) => {
                let target_node = unsafe { target_node.as_mut() };

                has_perms(auth_mgr, user_id, target_node.perms())?;

                if !target_node.is_directory() {
                    return Err(HFSIoError::NoSuchFileOrDirectory);
                }

                if src_parent == target_path {
                    return Ok(());
                }

                let parent_node = fs_dir
                    .get_node_mut(&src_parent)
                    .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

                has_perms(auth_mgr, user_id, parent_node.perms())?;

                let name = src_path.last_path_name().unwrap();
                let Some(node) = parent_node.remove_child(name) else {
                    return Err(HFSIoError::NoSuchFileOrDirectory);
                };

                let node = unsafe { Box::from_raw(node.as_ptr()) };
                if let Some(src_node) = target_node.add_child(node) {
                    let _ = parent_node.add_child(src_node);
                    return Err(HFSIoError::ExistsFileOrDirectory);
                }
            }
            None => {
                let target_parent = target_path
                    .parent_path()
                    .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

                let Some(mut target_node) = fs_dir.get_node_ptr(&target_path) else {
                    return Err(HFSIoError::NoSuchFileOrDirectory);
                };
                let target_node = unsafe { target_node.as_mut() };

                has_perms(auth_mgr, user_id, target_node.perms())?;

                let name = src_path.last_path_name().unwrap();
                if target_parent == src_parent {
                    let Some(node) = target_node.remove_child(name) else {
                        return Err(HFSIoError::NoSuchFileOrDirectory);
                    };
                    let mut node = unsafe { Box::from_raw(node.as_ptr()) };

                    let new_name = target_path.last_path_name().unwrap();
                    node.rename(new_name);
                    let _ = target_node.add_child(node);
                } else {
                    let parent_node = fs_dir
                        .get_node_mut(&src_parent)
                        .ok_or(HFSIoError::NoSuchFileOrDirectory)?;

                    has_perms(auth_mgr, user_id, parent_node.perms())?;

                    let Some(node) = parent_node.remove_child(name) else {
                        return Err(HFSIoError::NoSuchFileOrDirectory);
                    };
                    let mut node = unsafe { Box::from_raw(node.as_ptr()) };

                    let new_name = target_path.last_path_name().unwrap();
                    node.rename(new_name);
                    let _ = target_node.add_child(node);
                }
            }
        }

        Ok(())
    }

    pub fn delete_file_cache(&self, file_id: &i64) {
        let mut open_cache = self.open_cache.lock().unwrap();
        let _ = open_cache.remove(file_id);
    }

    pub fn file_cache_update(&self, file_id: &i64, chunks: Vec<i64>) {
        let mut open_cache = self.open_cache.lock().unwrap();

        if let Some(ptr) = open_cache.get_mut(file_id) {
            let node = unsafe { ptr.as_mut() };
            node.node_info = NodeInfo::File(chunks);
        }
    }

    fn next_node_id(&self) -> i64 {
        self.gen_id.next_id()
    }

    pub async fn serilize<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> crate::HFSResult<()> {
        writer.write_i64(self.next_node_id()).await?;
        let fd = self.fs_directory.lock().unwrap();

        fd_serialize(&fd, writer).await
    }
}

fn has_perms(auth_mgr: &AuthManager, user_id: u16, perms: &Permissions) -> Result<(), HFSIoError> {
    if let OPPermissions::ReadOnly | OPPermissions::NoPermissions =
        auth_mgr.verify_permissions(user_id, perms)
    {
        return Err(HFSIoError::NoPermissions);
    }

    Ok(())
}

impl FSDirectory {
    pub fn init() -> FSDirectory {
        let mut super_root = Box::new(Node::new(0, vec![], NodeType::DirectoryNode));

        let mut root = Box::new(Node::new(0, vec![], NodeType::DirectoryNode));
        root.set_perms(Permissions::with_id(0, 0));

        let mut home = Box::new(Node::new(0, b"home".to_vec(), NodeType::DirectoryNode));
        home.set_perms(Permissions::with_id(0, 0));

        root.add_child(home);

        super_root.add_child(root);

        FSDirectory {
            super_root: box_to_ptr(super_root),
            count: 1,
            marker: PhantomData,
        }
    }

    pub fn from_ptr(ptr: NonNull<Node>) -> FSDirectory {
        let super_root = Box::new(Node::with_info(0, vec![], NodeInfo::Directory(vec![ptr])));

        let mut stack = vec![ptr];
        let mut count = 0;
        while let Some(node_ptr) = stack.pop() {
            count += 1;
            if let Some(child_node) = unsafe { node_ptr.as_ref().child_node() } {
                stack.extend_from_slice(child_node);
            }
        }

        FSDirectory {
            super_root: box_to_ptr(super_root),
            count,
            marker: PhantomData,
        }
    }

    pub fn all_chunks(&self) -> Vec<i64> {
        let mut stack = vec![self.super_root];
        let mut chunk_ids = vec![];

        while let Some(ptr) = stack.pop() {
            if let Some(childs) = unsafe { ptr.as_ref().child_node() } {
                for child_ptr in childs.iter() {
                    let child_node = unsafe { child_ptr.as_ref() };

                    if let NodeInfo::File(ids) = &child_node.node_info {
                        chunk_ids.extend_from_slice(ids);
                    }
                }
                stack.extend_from_slice(childs);
            }
        }

        chunk_ids
    }

    pub fn _count(&self) -> usize {
        self.count
    }

    pub fn get_node(&self, src: &HFSPath) -> Option<&Node> {
        let mut path_components = src.components();
        let mut crt_node = unsafe { self.super_root.as_ref() };
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
        let mut crt_node = unsafe { (&mut self.super_root).as_mut() };
        while let Some(name) = path_components.next() {
            if let Some(child_node) = crt_node.get_child_mut(name) {
                crt_node = child_node;
            } else {
                return None;
            }
        }

        Some(crt_node)
    }

    fn get_node_ptr(&self, src: &HFSPath) -> Option<NonNull<Node>> {
        let mut path_components = src.components();
        let mut cur_node = self.super_root;
        while let Some(name) = path_components.next() {
            if let Some(child_ptr) = unsafe { cur_node.as_ref().get_child_ptr(name) } {
                cur_node = child_ptr;
            } else {
                return None;
            }
        }

        Some(cur_node)
    }

    fn _rename_node(&mut self, src: &HFSPath, new_name: &[u8]) -> bool {
        let Some(parent_path) = src.parent_path() else {
            return false;
        };

        let Some(parent_node) = self.get_node_mut(&parent_path) else {
            return false;
        };

        if parent_node.get_child(new_name).is_some() {
            return false;
        }

        let node_name = src.last_path_name().unwrap();
        let Some(node) = parent_node.get_child_mut(node_name) else {
            return false;
        };

        node.rename(new_name);
        true
    }
}

pub async fn fd_serialize<W: AsyncWriteExt + Unpin>(
    fs: &FSDirectory,
    writer: &mut W,
) -> crate::HFSResult<()> {
    let mut queue = VecDeque::new();

    let super_root = unsafe { fs.super_root.as_ref() };
    queue.push_back(super_root);

    while let Some(node) = queue.pop_front() {
        let mut buf = Vec::new();

        match &node.node_info {
            NodeInfo::Directory(node_vec) => {
                buf.extend(node_vec.len().to_be_bytes());
                for item in node_vec {
                    let child = unsafe { item.as_ref() };
                    node_to_bytes(child, &mut buf);
                    queue.push_back(child);
                }
            }
            NodeInfo::File(_) => {
                buf.extend(0usize.to_be_bytes());
            }
        }

        writer.write_all(&buf).await?;
    }

    writer.flush().await?;

    Ok(())
}

pub async fn fd_deserialize<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> crate::HFSResult<FSDirectory> {
    let super_root_ptr = box_to_ptr(Box::new(Node::new(0, vec![], NodeType::DirectoryNode)));

    let mut count = 0;

    let mut queue = VecDeque::new();
    queue.push_back(super_root_ptr);

    while let Some(mut node_ptr) = queue.pop_front() {
        let child_size = reader.read_u64().await? as usize;
        let node = unsafe { node_ptr.as_mut() };

        for _ in 0..child_size {
            let child_node = node_from_reader(reader).await?;
            let child_node_ptr = box_to_ptr(child_node);

            queue.push_back(child_node_ptr);
            count += 1;

            unsafe {
                if let Some(_) = node.add_child(Box::from_raw(child_node_ptr.as_ptr())) {
                    return Err("FSDirector error".into());
                }
            }
        }
    }

    Ok(FSDirectory {
        super_root: super_root_ptr,
        count,
        marker: PhantomData,
    })
}

async fn node_from_reader<R: AsyncReadExt + Unpin>(reader: &mut R) -> crate::HFSResult<Box<Node>> {
    let mut buf = [0u8; 39];
    reader.read_exact(&mut buf).await?;

    let name_len = usize::from_be_bytes(buf[31..39].try_into().unwrap());
    let mut buf2 = vec![0u8; name_len + 1];
    reader.read_exact(&mut buf2).await?;

    let byte = buf2.pop().unwrap();
    match byte {
        1 => {
            let perms =
                Permissions::from_bytes(&buf[24..31]).ok_or("Node perms deserialize error")?;

            Ok(Box::new(Node {
                node_id: i64::from_be_bytes(buf[0..8].try_into().unwrap()),
                name: buf2,
                node_info: NodeInfo::Directory(vec![]),
                mdfc_time: u64::from_be_bytes(buf[8..16].try_into().unwrap()),
                acs_time: u64::from_be_bytes(buf[16..24].try_into().unwrap()),
                perms,
            }))
        }
        2 => {
            let chunk_len = reader.read_u64().await? as usize;
            let mut chunk_buf = vec![0u8; chunk_len * 8];
            reader.read_exact(&mut chunk_buf).await?;

            let chunks: Vec<i64> = chunk_buf
                .chunks(8)
                .map(|c| i64::from_be_bytes(c.try_into().unwrap()))
                .collect();

            let perms =
                Permissions::from_bytes(&buf[24..31]).ok_or("Node perms deserialize error")?;

            Ok(Box::new(Node {
                node_id: i64::from_be_bytes(buf[0..8].try_into().unwrap()),
                name: buf2,
                node_info: NodeInfo::File(chunks),
                mdfc_time: u64::from_be_bytes(buf[8..16].try_into().unwrap()),
                acs_time: u64::from_be_bytes(buf[16..24].try_into().unwrap()),
                perms,
            }))
        }
        _ => Err("Node deserialize error".into()),
    }
}

fn node_to_bytes(node: &Node, buf: &mut Vec<u8>) {
    buf.extend(node.node_id.to_be_bytes());
    buf.extend(node.mdfc_time.to_be_bytes());
    buf.extend(node.acs_time.to_be_bytes());
    buf.extend(node.perms.to_bytes());
    buf.extend(node.name.len().to_be_bytes());
    for byte in &node.name {
        buf.push(*byte);
    }

    match &node.node_info {
        NodeInfo::Directory(_) => {
            buf.push(1);
        }
        NodeInfo::File(chunks) => {
            buf.push(2);
            buf.extend(chunks.len().to_be_bytes());
            for item in chunks {
                buf.extend(item.to_be_bytes());
            }
        }
    }
}

impl Drop for FSDirectory {
    fn drop(&mut self) {
        struct DropGuard<'a>(&'a mut Vec<NonNull<Node>>);

        impl<'a> Drop for DropGuard<'a> {
            fn drop(&mut self) {
                // Continue the same loop we do below. This only runs when a destructor has
                // panicked. If another one panics this will abort.
                while let Some(node) = self.0.pop() {
                    let mut node = unsafe { Box::from_raw(node.as_ptr()) };
                    if let Some(child_node) = node.child_node_mut() {
                        self.0.append(child_node);
                    }
                }
            }
        }

        // Wrap self so that if a destructor panics, we can try to keep looping
        let mut stack = vec![self.super_root];
        let guard = DropGuard(&mut stack);
        while let Some(node) = guard.0.pop() {
            let mut node = unsafe { Box::from_raw(node.as_ptr()) };
            if let Some(child_node) = node.child_node_mut() {
                guard.0.append(child_node);
            }
        }
        std::mem::forget(guard);
    }
}

impl Debug for FSDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FSDirectory")
            .field("super_root", unsafe { &self.super_root.as_ref() })
            .field("count", &self.count)
            .field("marker", &self.marker)
            .finish()
    }
}

unsafe impl Send for FileSystem {}
unsafe impl Sync for FileSystem {}
