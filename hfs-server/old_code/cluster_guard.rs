use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use hfs_proto::{cluster::ReportInfo, master::ReplicaInfo as PMReplicaInfo};
use tokio::time::{Duration, Instant};
use volo::FastStr;

use crate::{chunk::ReplicaInfo, util::gen_id::GenIdU32};

use super::{chunk_instance::ChunkInstance, error::ClusterError};

#[derive(Debug)]
pub struct ClusterGuard {
    server_instance: Mutex<AHashMap<u32, ChunkInstance>>,
    gen_id: GenIdU32,
}

impl ClusterGuard {
    pub fn new() -> ClusterGuard {
        ClusterGuard {
            server_instance: Mutex::new(AHashMap::new()),
            gen_id: GenIdU32::new(),
        }
    }

    pub fn report_heartbeat(&self, report_info: ReportInfo) -> Result<(), ClusterError> {
        let mut servers = self.server_instance.lock().unwrap();
        let report_server = servers
            .get_mut(&report_info.server_id)
            .ok_or(ClusterError::UnknownServer)?;
        report_server.update_with_info(report_info);
        Ok(())
    }

    pub fn join_cluster(&self, server_addr: FastStr) -> u32 {
        let mut servers = self.server_instance.lock().unwrap();
        let server_id = self.gen_id.next_id();
        servers.insert(
            server_id,
            ChunkInstance::new(server_id, server_addr.to_string()),
        );
        server_id
    }

    pub fn to_rpc_replicas(&self, reps: Vec<Vec<ReplicaInfo>>) -> Option<Vec<Vec<PMReplicaInfo>>> {
        let servers = self.server_instance.lock().unwrap();
        let mut pmreps = vec![];
        for rep in reps {
            let mut preps = vec![];
            for mrep in rep {
                if let Some(serv) = servers.get(&mrep.server_id) {
                    preps.push(PMReplicaInfo {
                        address: FastStr::from(serv.addr().to_string()),
                        chunk_id: mrep.chunk_id,
                    });
                }
            }
            if preps.is_empty() {
                return None;
            }
            pmreps.push(preps);
        }
        Some(pmreps)
    }

    fn check_live(&self) {
        let mut servers = self.server_instance.lock().unwrap();
        let current_time = std::time::Instant::now();
        servers.retain(|_, inst| inst.is_live(&current_time));
    }
}

pub async fn check_heartbeat_task(cluster_gurad: Arc<ClusterGuard>) {
    static CHECK_INTERVAL_TIME: Duration = Duration::from_secs(30);
    loop {
        cluster_gurad.check_live();

        let next_check_time = Instant::now() + CHECK_INTERVAL_TIME;
        tokio::time::sleep_until(next_check_time).await;
    }
}
