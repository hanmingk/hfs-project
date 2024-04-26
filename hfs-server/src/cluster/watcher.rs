use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use ahash::AHashMap;
use hfs_proto::cluster::{HeartbeatRequest, ServiceType};

use crate::{shutdown::Shutdown, util::gen_id::GenIdU32};

use super::{
    cfg::MAX_LIVE_TIME,
    server::{self, ChunkServer},
};

#[derive(Debug)]
pub struct ClusterWatcher {
    members: Mutex<AHashMap<u32, ChunkServer>>,
    member_status: Mutex<AHashMap<u32, ServerStatus>>,
    gen_id: GenIdU32,
}

#[derive(Debug)]
struct ServerStatus {
    last_rt: Instant,
    cpu_load: f32,
    disk_load: f32,
    network_load: f32,
}

impl ClusterWatcher {
    pub fn new() -> ClusterWatcher {
        ClusterWatcher {
            members: Mutex::new(AHashMap::new()),
            member_status: Mutex::new(AHashMap::new()),
            gen_id: GenIdU32::new(1),
        }
    }

    pub fn service_socket_addr(
        &self,
        server_id: &u32,
        service_type: &ServiceType,
    ) -> Result<String, Error> {
        let members = self.members.lock().unwrap();
        members
            .get(server_id)
            .map(|server| {
                format!(
                    "{}.{}.{}.{}:{}",
                    server.addr[0],
                    server.addr[1],
                    server.addr[2],
                    server.addr[3],
                    server.service_ports.get(service_type).unwrap()
                )
            })
            .ok_or(Error::UnknownServer)
    }

    pub fn member_add(&self, mut server: ChunkServer) -> Result<u32, Error> {
        let mut members = self.members.lock().unwrap();
        if server.server_id == 0 {
            server.server_id = self.next_id();
        }

        let server_id = server.server_id;
        members.insert(server_id, server);

        let mut member_status = self.member_status.lock().unwrap();
        member_status.insert(
            server_id,
            ServerStatus {
                last_rt: Instant::now(),
                cpu_load: 0.0,
                disk_load: 0.0,
                network_load: 0.0,
            },
        );
        Ok(server_id)
    }

    pub fn heartbeat(&self, info: HeartbeatRequest) -> Result<(), Error> {
        let mut member_status = self.member_status.lock().unwrap();
        let Some(server_status) = member_status.get_mut(&info.server_id) else {
            return Err(Error::UnknownServer);
        };

        server_status.update(info);
        Ok(())
    }

    pub fn member_remove(&self, server_id: u32) {
        let mut members = self.members.lock().unwrap();
        members.remove(&server_id);
        let mut member_status = self.member_status.lock().unwrap();
        member_status.remove(&server_id);
    }

    pub fn next_replica_server_id(&self, num: usize) -> Vec<u32> {
        let member_status = self.member_status.lock().unwrap();
        let all_server_id: Vec<u32> = member_status.iter().map(|(id, _)| *id).collect();

        let mut ids = vec![];

        let mut index = 0;
        for _ in 0..num {
            ids.push(all_server_id[index]);
            index = (index + 1) % all_server_id.len();
        }

        ids
    }

    pub fn is_availabel(&self, server_id: &u32) -> bool {
        let members = self.members.lock().unwrap();
        members
            .get(server_id)
            .map(|cserver| cserver.status == server::ServerStatus::Online)
            .is_some_and(|b| b)
    }

    fn next_id(&self) -> u32 {
        self.gen_id.next_id()
    }
}

pub async fn check_heartbeat_task(cluster_watcher: Arc<ClusterWatcher>, mut shutdown: Shutdown) {
    while !shutdown.is_shutdown() {
        {
            let mut member_status = cluster_watcher.member_status.lock().unwrap();
            let current_time = Instant::now();
            member_status.retain(|id, status| {
                let is_live = status.is_live(&current_time);
                if !is_live {
                    let mut members = cluster_watcher.members.lock().unwrap();
                    if let Some(server) = members.get_mut(id) {
                        server.status = server::ServerStatus::Offline;

                        tracing::warn!("Chunk server offline: {:?}", server);
                    }
                }
                is_live
            });
        }

        let next_check_time = tokio::time::Instant::now() + MAX_LIVE_TIME;
        tokio::select! {
            _ = tokio::time::sleep_until(next_check_time) => {},
            _ = shutdown.recv() => {
                break;
            }
        };
    }
}

impl ServerStatus {
    fn is_live(&self, cur_time: &Instant) -> bool {
        cur_time.duration_since(self.last_rt) < MAX_LIVE_TIME
    }

    fn update(&mut self, info: HeartbeatRequest) {
        self.last_rt = Instant::now();
        self.cpu_load = info.cpu_load;
        self.disk_load = info.disk_load;
        self.network_load = info.network_load;
    }
}

#[derive(Debug)]
pub enum Error {
    UnknownServer,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnknownServer => "Unknown chunk server".fmt(f),
        }
    }
}

impl From<Error> for volo_grpc::Status {
    fn from(value: Error) -> Self {
        volo_grpc::Status::new(volo_grpc::Code::Unknown, value.to_string())
    }
}
