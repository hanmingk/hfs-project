use std::sync::Mutex;

use ahash::AHashMap;
use hfs_proto::chunk::{Status, StatusReportRequest};

use crate::{
    grpc_client::MASTER_CHUNK_CLIENT, transport::replica_file::ReplicaFile, util::gen_id::GenIdI64,
};

use super::replica::ReplicaMetaInfo;

#[derive(Debug)]
pub struct ReplicaManager {
    pub replica_meta: Mutex<AHashMap<i64, ReplicaMetaInfo>>,
    pub update_map: Mutex<AHashMap<i64, ReplicaMetaInfo>>,
    pub server_id: u32,
    pub gen_id: GenIdI64,
    pub base_dir: String,
}

impl ReplicaManager {
    pub fn contains_key(&self, replica_id: &i64) -> bool {
        let replica_meta = self.replica_meta.lock().unwrap();

        replica_meta.contains_key(replica_id)
    }

    pub fn remove_replicas(&self, replica_ids: &[i64]) -> Vec<i64> {
        let mut replica_meta = self.replica_meta.lock().unwrap();

        replica_ids
            .iter()
            .filter(|&id| replica_meta.remove(id).is_some())
            .cloned()
            .collect()
    }

    pub fn add_meta(&self, meta: ReplicaMetaInfo) {
        let mut replica_meta = self.replica_meta.lock().unwrap();

        if replica_meta.contains_key(&meta.replica_id) {
            return;
        }

        replica_meta.insert(meta.replica_id, meta);
    }

    pub fn update_status(&self, replica_id: &i64, status: Status) {
        let mut replica_meta = self.replica_meta.lock().unwrap();

        if let Some(meta_info) = replica_meta.get_mut(replica_id) {
            meta_info.status = status
        }
    }

    pub fn get_replica_meta(&self, replica_id: &i64) -> Option<ReplicaMetaInfo> {
        let replica_meta = self.replica_meta.lock().unwrap();

        replica_meta
            .get(replica_id)
            .and_then(|replica_info| {
                (replica_info.status == Status::Complete).then_some(replica_info)
            })
            .cloned()
    }

    pub fn get_update_info(&self, replica_id: &i64) -> Option<ReplicaMetaInfo> {
        let update_map = self.update_map.lock().unwrap();

        update_map.get(replica_id).cloned()
    }

    pub fn update_complete(&self, replica_id: &i64) -> Option<ReplicaMetaInfo> {
        let mut update_map = self.update_map.lock().unwrap();

        update_map.remove(replica_id)
    }

    pub fn update_replica(&self, replica_id: &i64) -> Option<ReplicaMetaInfo> {
        let mut replica_meta = self.replica_meta.lock().unwrap();

        if let Some(meta_info) = replica_meta.remove(replica_id) {
            let mut update_map = self.update_map.lock().unwrap();
            update_map.insert(meta_info.replica_id, meta_info.clone());

            return Some(meta_info);
        }

        None
    }

    pub fn next_replica_id(&self) -> i64 {
        self.gen_id.next_id()
    }
}

pub async fn start_report(replica_mgr: &ReplicaManager) {
    let replicas: Vec<_> = replica_mgr
        .replica_meta
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.1.clone())
        .collect();

    for item in replicas {
        let _ = MASTER_CHUNK_CLIENT
            .status_report(StatusReportRequest {
                chunk_id: item.chunk_id,
                replica_id: item.replica_id,
                num_bytes: item.num_bytes,
                size: item.size,
                version: item.version,
                server_id: replica_mgr.server_id,
                is_update: true,
            })
            .await;
    }
}

pub async fn update_complete(replica_id: i64, replica_mgr: &ReplicaManager, rollback: bool) {
    if replica_mgr.update_complete(&replica_id).is_none() {
        return;
    }

    load_meta(replica_id, replica_mgr, rollback).await;
}

pub async fn load_meta(replica_id: i64, replica_mgr: &ReplicaManager, rollback: bool) {
    let Some(mut meta_info) = ReplicaMetaInfo::from_file(&replica_mgr.base_dir, replica_id).await
    else {
        return;
    };

    if rollback {
        if let Ok(mut replica_file) =
            ReplicaFile::from(&replica_mgr.base_dir, meta_info.clone()).await
        {
            if let Ok(Some(last_info)) = replica_file.rollback().await {
                meta_info = last_info;
            } else {
                return;
            }
        }
    }

    if replica_mgr.contains_key(&meta_info.replica_id) {
        return;
    }

    if MASTER_CHUNK_CLIENT
        .status_report(StatusReportRequest {
            chunk_id: meta_info.chunk_id,
            replica_id: meta_info.replica_id,
            num_bytes: meta_info.num_bytes,
            size: meta_info.size,
            version: meta_info.version,
            server_id: replica_mgr.server_id,
            is_update: rollback,
        })
        .await
        .is_ok()
    {
        replica_mgr.add_meta(meta_info);
    }
}
