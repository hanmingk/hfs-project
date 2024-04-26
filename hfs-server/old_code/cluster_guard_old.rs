use std::{
    sync::{atomic::AtomicU32, Mutex},
    time::Instant,
};

use ahash::AHashMap;
use faststr::FastStr;
use hfs_proto::cluster::ChunkServerInfo;

use super::chunk_server::ChunkServer;

#[derive(Debug)]
pub struct ClusterGuard {
    pub servers: Mutex<AHashMap<u32, ChunkServer>>,
    server_heartbeat: Mutex<AHashMap<u32, Instant>>,
    atomic: AtomicU32,
}

impl ClusterGuard {
    pub fn new() -> ClusterGuard {
        ClusterGuard {
            servers: Mutex::new(AHashMap::new()),
            server_heartbeat: Mutex::new(AHashMap::new()),
            atomic: AtomicU32::new(1),
        }
    }
    pub fn report_heartbeat(&self, server_info: ChunkServerInfo) -> Result<(), FastStr> {
        let mut servers = self.servers.lock().unwrap();
        if let Some(chunk_server) = servers.get_mut(&server_info.server_id) {
            let mut server_heartbeat = self.server_heartbeat.lock().unwrap();
            if let Some(last_time) = server_heartbeat.get_mut(&server_info.server_id) {
                *last_time = Instant::now();
            }
            chunk_server.update_with_info(server_info);
            return Ok(());
        }
        Err(FastStr::from("Unknown chunk server"))
    }

    pub fn join_cluster(&self, server_info: ChunkServerInfo) -> Result<u32, FastStr> {
        let server_id = self.next_id();
        let mut servers = self.servers.lock().unwrap();
        servers.insert(server_id, ChunkServer::with_info(server_info));
        let mut servere_heartbeat = self.server_heartbeat.lock().unwrap();
        servere_heartbeat.insert(server_id, Instant::now());
        Ok(server_id)
    }

    pub fn next_id(&self) -> u32 {
        let id = self
            .atomic
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if id == u32::MAX {
            panic!("Over `u32::MAX` server id created")
        }
        id
    }
}
