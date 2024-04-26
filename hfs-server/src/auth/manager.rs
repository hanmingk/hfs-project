use std::{
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use ahash::AHashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
};
use volo::FastStr;

use crate::{
    shutdown::Shutdown,
    util::search::{binary_search, BinarySearch},
};

use super::{
    cfg::{DEFAULT_ROOT_NAME, DEFAULT_ROOT_PASSWD, MAX_LIVE_TIME},
    AuthError, OPPermissions, Permissions,
};

#[derive(Debug)]
pub struct AuthManager {
    login_cache: Mutex<AHashMap<String, (u16, u16, Instant)>>,
    user_map: Mutex<Vec<UserInfo>>,
    group_map: Mutex<AHashMap<u16, GroupInfo>>,
    user_atomic: AtomicU16,
    group_atomic: AtomicU16,
    backgroud_task: Notify,
}

#[derive(Debug, Clone)]
pub struct UserInfo {
    pub user_id: u16,
    pub group_id: u16,
    pub username: String,
    pub passwd: String,
}

impl UserInfo {
    pub fn root() -> UserInfo {
        UserInfo {
            user_id: 0,
            group_id: 0,
            username: String::from(DEFAULT_ROOT_NAME),
            passwd: String::from(DEFAULT_ROOT_PASSWD),
        }
    }
}

#[derive(Debug)]
struct GroupInfo {
    #[allow(dead_code)]
    group_id: u16,
    group_name: String,
    users: Vec<u16>,
}

impl GroupInfo {
    pub fn root() -> GroupInfo {
        GroupInfo {
            group_id: 0,
            group_name: String::from(DEFAULT_ROOT_NAME),
            users: vec![0],
        }
    }
}

impl AuthManager {
    pub fn new() -> AuthManager {
        AuthManager {
            login_cache: Mutex::new(AHashMap::new()),
            user_map: Mutex::new(vec![UserInfo::root()]),
            group_map: Mutex::new(AHashMap::from([(0, GroupInfo::root())])),
            user_atomic: AtomicU16::new(1),
            group_atomic: AtomicU16::new(1),
            backgroud_task: Notify::new(),
        }
    }

    pub fn sign_in(&self, username: &str, passwd: &str) -> Result<FastStr, AuthError> {
        let user_map = self.user_map.lock().unwrap();
        if let (true, index) = binary_search(&user_map, username) {
            let user_info = unsafe { user_map.get_unchecked(index) };
            if &user_info.passwd == passwd {
                let mut login_cache = self.login_cache.lock().unwrap();
                let token = format!("{}:{}", username, passwd);
                login_cache.insert(
                    token.clone(),
                    (user_info.user_id, user_info.group_id, Instant::now()),
                );

                if login_cache.len() == 1 {
                    self.backgroud_task.notify_one();
                }

                return Ok(FastStr::from_string(token));
            }
        }
        Err(AuthError::UserNameOrPasswordError)
    }

    pub fn keep_heartbeat(&self, token: &str) -> Result<(), AuthError> {
        let mut login_cache = self.login_cache.lock().unwrap();

        login_cache
            .get_mut(token)
            .map(|(_, _, time)| *time = Instant::now())
            .ok_or(AuthError::UnSign)
    }

    pub fn create_user(
        &self,
        root_token: &str,
        username: &str,
        passwd: &str,
    ) -> Result<(u16, u16), AuthError> {
        if !self.verify_root_identity(root_token) {
            return Err(AuthError::PermissionDenied);
        }
        let mut user_map = self.user_map.lock().unwrap();
        if let (false, index) = binary_search(&user_map, username) {
            let mut group_map = self.group_map.lock().unwrap();

            if contains_group_name(&group_map, username).is_some() {
                return Err(AuthError::GroupNameExists);
            }

            let group_id = self.next_group_id();
            let user_id = self.next_user_id();

            group_map.insert(
                group_id,
                GroupInfo {
                    group_id,
                    group_name: username.to_string(),
                    users: vec![user_id],
                },
            );

            user_map.insert(
                index,
                UserInfo {
                    user_id,
                    group_id,
                    username: username.to_string(),
                    passwd: passwd.to_string(),
                },
            );
            return Ok((user_id, group_id));
        }
        Err(AuthError::UserNameExists)
    }

    pub fn remove_user(&self, root_token: &str, username: &str) -> Result<(), AuthError> {
        if !self.verify_root_identity(root_token) {
            return Err(AuthError::PermissionDenied);
        }
        let mut user_map = self.user_map.lock().unwrap();
        if let (true, index) = binary_search(&user_map, username) {
            let user_info = user_map.remove(index);
            let mut group_map = self.group_map.lock().unwrap();
            group_map.remove(&user_info.group_id);
        }
        Ok(())
    }

    pub fn create_group(&self, root_token: &str, group_name: &str) -> Result<(), AuthError> {
        if !self.verify_root_identity(root_token) {
            return Err(AuthError::PermissionDenied);
        }

        let mut group_map = self.group_map.lock().unwrap();
        if contains_group_name(&group_map, group_name).is_some() {
            return Err(AuthError::GroupNameExists);
        }
        let new_group_id = self.next_group_id();
        group_map.insert(
            new_group_id,
            GroupInfo {
                group_id: new_group_id,
                group_name: group_name.to_string(),
                users: vec![],
            },
        );
        Ok(())
    }

    pub fn remove_group(&self, root_token: &str, group_name: &str) -> Result<(), AuthError> {
        if !self.verify_root_identity(root_token) {
            return Err(AuthError::PermissionDenied);
        }

        let mut group_map = self.group_map.lock().unwrap();
        if let Some(group_id) = contains_group_name(&group_map, group_name) {
            group_map.remove(&group_id);
        }
        Ok(())
    }

    pub fn add_user_to_group(
        &self,
        root_token: &str,
        group_name: &str,
        username: &str,
    ) -> Result<(), AuthError> {
        if !self.verify_root_identity(root_token) {
            return Err(AuthError::PermissionDenied);
        }
        let user_map = self.user_map.lock().unwrap();
        if let (true, index) = binary_search(&user_map, username) {
            let user_id = unsafe { user_map.get_unchecked(index).user_id };
            let mut group_map = self.group_map.lock().unwrap();
            if let Some(group_id) = contains_group_name(&group_map, group_name) {
                let group_info = group_map.get_mut(&group_id).unwrap();
                group_info.insert_user(user_id);
            }
        }
        Ok(())
    }

    pub fn remove_user_from_group(
        &self,
        root_token: &str,
        group_name: &str,
        username: &str,
    ) -> Result<(), AuthError> {
        if !self.verify_root_identity(root_token) {
            return Err(AuthError::PermissionDenied);
        }
        let user_map = self.user_map.lock().unwrap();
        if let (true, index) = binary_search(&user_map, username) {
            let user_id = unsafe { &user_map.get_unchecked(index).user_id };
            let mut group_map = self.group_map.lock().unwrap();
            if let Some(group_id) = contains_group_name(&group_map, group_name) {
                let group_info = group_map.get_mut(&group_id).unwrap();
                group_info.remove_user(user_id);
            }
        }
        Ok(())
    }

    pub fn verify_identity(&self, auth_token: &str) -> Option<(u16, u16)> {
        let login_cache = self.login_cache.lock().unwrap();
        login_cache
            .get(auth_token)
            .map(|(user_id, group_id, _)| (*user_id, *group_id))
    }

    pub fn verify_permissions(&self, owner_id: u16, perms: &Permissions) -> OPPermissions {
        if owner_id == 0 {
            // root
            return OPPermissions::ReadWithWrite;
        }
        if owner_id == perms.owner_id {
            return perms.op_perms[0];
        }
        let group_map = self.group_map.lock().unwrap();
        if let Some(group) = group_map.get(&perms.group_id) {
            if group.contains_user(&perms.owner_id) {
                return perms.op_perms[1];
            }
        }
        perms.op_perms[2]
    }

    pub fn get_group_name(&self, group_id: u16) -> Option<String> {
        let group_map = self.group_map.lock().unwrap();
        if let Some(group_info) = group_map.get(&group_id) {
            return Some(group_info.group_name.clone());
        }
        None
    }

    pub fn get_user_name(&self, user_id: u16) -> Option<String> {
        let user_map = self.user_map.lock().unwrap();
        for user_info in user_map.iter() {
            if user_info.user_id == user_id {
                return Some(user_info.username.clone());
            }
        }

        None
    }

    fn check_login_live(&self) -> bool {
        let mut login_cache = self.login_cache.lock().unwrap();

        let current_time = std::time::Instant::now();
        login_cache.retain(|_, cache| current_time.duration_since(cache.2) < MAX_LIVE_TIME);

        login_cache.is_empty()
    }

    fn verify_root_identity(&self, auth_token: &str) -> bool {
        let login_cache = self.login_cache.lock().unwrap();
        if let Some(user_info) = login_cache.get(auth_token) {
            if user_info.0 == 0 {
                return true;
            }
        }
        false
    }

    fn next_user_id(&self) -> u16 {
        let id = self.user_atomic.fetch_add(1, Ordering::Relaxed);
        if id == u16::MAX {
            panic!("Over `i64::MAX` node id created.");
        }
        id
    }

    fn next_group_id(&self) -> u16 {
        let id = self.group_atomic.fetch_add(1, Ordering::Relaxed);
        if id == u16::MAX {
            panic!("Over `i64::MAX` node id created.");
        }
        id
    }

    pub async fn serilize<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> crate::HFSResult<()> {
        auth_serialize(self, writer).await
    }
}

impl BinarySearch for UserInfo {
    type Key = str;

    fn get_key(&self) -> &Self::Key {
        &self.username
    }
}

impl BinarySearch for u16 {
    type Key = u16;

    fn get_key(&self) -> &Self::Key {
        &self
    }
}

impl GroupInfo {
    fn contains_user(&self, user_id: &u16) -> bool {
        binary_search(&self.users, user_id).0
    }

    fn insert_user(&mut self, user_id: u16) {
        if let (false, index) = binary_search(&self.users, &user_id) {
            self.users.insert(index, user_id);
        }
    }

    fn remove_user(&mut self, user_id: &u16) {
        if let (true, index) = binary_search(&self.users, &user_id) {
            self.users.remove(index);
        }
    }
}

fn contains_group_name(group_map: &AHashMap<u16, GroupInfo>, group_name: &str) -> Option<u16> {
    for (id, group_info) in group_map.iter() {
        if &group_info.group_name == group_name {
            return Some(*id);
        }
    }
    None
}

pub async fn check_login_cache_live(auth_mgr: Arc<AuthManager>, mut shutdown: Shutdown) {
    use tokio::time::{Duration, Instant};

    const CHECK_INTERVAL_TIME: Duration = Duration::from_secs(30);

    while shutdown.is_shutdown() {
        tokio::select! {
            _ = async {
                if auth_mgr.check_login_live() {
                    auth_mgr.backgroud_task.notified().await;
                }

                let next_check_time = Instant::now() + CHECK_INTERVAL_TIME;
                tokio::time::sleep_until(next_check_time).await;
            } => (),
            _ = shutdown.recv() => {
                tracing::info!("auth check task shutdown");
            }
        }
    }
}

pub async fn auth_serialize<W: AsyncWriteExt + Unpin>(
    auth_mgr: &AuthManager,
    writer: &mut W,
) -> crate::HFSResult<()> {
    let mut header_buf = [0u8; 4];
    header_buf[0..2].copy_from_slice(&auth_mgr.next_user_id().to_be_bytes());
    header_buf[2..4].copy_from_slice(&auth_mgr.next_group_id().to_be_bytes());
    writer.write_all(&header_buf).await?;

    let user_map = auth_mgr.user_map.lock().unwrap();
    writer.write_u64(user_map.len() as u64).await?;
    for item in user_map.iter() {
        let name_bytes = item.username.as_bytes();
        let passwd_bytes = item.passwd.as_bytes();

        let mut buf = Vec::with_capacity(20 + name_bytes.len() + passwd_bytes.len());

        buf.extend(item.user_id.to_be_bytes());
        buf.extend(item.group_id.to_be_bytes());
        buf.extend(name_bytes.len().to_be_bytes());
        buf.extend(passwd_bytes.len().to_be_bytes());
        buf.extend(name_bytes);
        buf.extend(passwd_bytes);

        writer.write_all(&mut buf).await?;
    }

    let group_map = auth_mgr.group_map.lock().unwrap();
    writer.write_u64(group_map.len() as u64).await?;
    for item in group_map.iter() {
        let name_bytes = item.1.group_name.as_bytes();

        let mut buf = Vec::with_capacity(18 + name_bytes.len() + item.1.users.len() * 2);

        buf.extend(item.1.group_id.to_be_bytes());
        buf.extend(name_bytes.len().to_be_bytes());
        buf.extend(item.1.users.len().to_be_bytes());
        buf.extend(name_bytes);
        for id in item.1.users.iter() {
            buf.extend(id.to_be_bytes());
        }

        writer.write_all(&mut buf).await?;
    }

    writer.flush().await?;

    Ok(())
}

pub async fn auth_deserialize<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> crate::HFSResult<AuthManager> {
    let mut header_buf = [0u8; 12];

    reader.read_exact(&mut header_buf).await?;

    let atomic_user_id = u16::from_be_bytes(header_buf[0..2].try_into().unwrap());
    let atomic_group_id = u16::from_be_bytes(header_buf[2..4].try_into().unwrap());

    let user_len = usize::from_be_bytes(header_buf[4..12].try_into().unwrap());
    let mut user_map = Vec::with_capacity(user_len);
    for _ in 0..user_len {
        let mut buf1 = [0u8; 20];
        reader.read_exact(&mut buf1).await?;

        let name_len = usize::from_be_bytes(buf1[4..12].try_into().unwrap());
        let mut bytes =
            vec![0u8; name_len + usize::from_be_bytes(buf1[12..20].try_into().unwrap())];
        reader.read_exact(&mut bytes).await?;

        let passwd_bytes = bytes.split_off(name_len);

        unsafe {
            user_map.push(UserInfo {
                user_id: u16::from_be_bytes(buf1[0..2].try_into().unwrap()),
                group_id: u16::from_be_bytes(buf1[2..4].try_into().unwrap()),
                username: String::from_utf8_unchecked(bytes),
                passwd: String::from_utf8_unchecked(passwd_bytes),
            });
        }
    }

    let group_len = reader.read_u64().await? as usize;
    let mut group_map = AHashMap::with_capacity(group_len);
    for _ in 0..group_len {
        let mut buf = [0u8; 18];
        reader.read_exact(&mut buf).await?;

        let name_len = usize::from_be_bytes(buf[2..10].try_into().unwrap());
        let ids_len = usize::from_be_bytes(buf[10..18].try_into().unwrap());
        let mut bytes = vec![0u8; name_len + ids_len * 2];

        reader.read_exact(&mut bytes).await?;

        let ids_bytes = bytes.split_off(name_len);

        let mut users = Vec::with_capacity(ids_len);
        for i in 0..ids_len {
            let start_index = i * 2;
            users.push(u16::from_be_bytes(
                ids_bytes[start_index..start_index + 2].try_into().unwrap(),
            ));
        }

        let group_info = unsafe {
            GroupInfo {
                group_id: u16::from_be_bytes(buf[0..2].try_into().unwrap()),
                group_name: String::from_utf8_unchecked(bytes),
                users,
            }
        };
        group_map.insert(group_info.group_id, group_info);
    }

    Ok(AuthManager {
        login_cache: Mutex::new(AHashMap::new()),
        user_map: Mutex::new(user_map),
        group_map: Mutex::new(group_map),
        user_atomic: AtomicU16::new(atomic_user_id),
        group_atomic: AtomicU16::new(atomic_group_id),
        backgroud_task: Notify::new(),
    })
}
