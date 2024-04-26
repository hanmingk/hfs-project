use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use ahash::AHashMap;
use hfs_proto::chunk::{ReplicaInfo, StatusReportRequest};
use tokio::sync::Notify;

use crate::{
    chunk::cfg::{DEFAULT_REPLICA_SIZE, NUM_REPLICA},
    cluster::ClusterWatcher,
    shutdown::Shutdown,
    util::gen_id::GenIdI64,
};

use super::meta::{ChunkMetaInfo, Status};

type Bucket = Mutex<AHashMap<i64, ChunkMetaInfo>>;

#[derive(Debug)]
pub struct ChunkManager {
    inner_storage: Vec<Bucket>,
    check_set: Mutex<HashSet<i64>>,
    gen_id: GenIdI64,
    #[allow(dead_code)]
    background_taks: Notify,
}

impl ChunkManager {
    pub fn new(bucket_size: usize, atomic: i64) -> ChunkManager {
        let mut inner_storage = Vec::with_capacity(bucket_size);
        for _ in 0..bucket_size {
            inner_storage.push(Mutex::new(AHashMap::new()))
        }

        ChunkManager {
            inner_storage,
            check_set: Mutex::new(HashSet::new()),
            gen_id: GenIdI64::new(atomic),
            background_taks: Notify::new(),
        }
    }

    pub fn bucket_size(&self) -> usize {
        self.inner_storage.len()
    }

    pub fn contains_id(&self, chunk_id: i64) -> bool {
        let target_bucket = unsafe {
            self.inner_storage
                .get_unchecked(self.bucket_index(chunk_id))
                .lock()
                .unwrap()
        };

        target_bucket.contains_key(&chunk_id)
    }

    pub fn add_chunk(&self) -> i64 {
        let chunk_id = self.next_id();
        let meta_info = ChunkMetaInfo {
            chunk_id,
            chunk_size: DEFAULT_REPLICA_SIZE as u32,
            num_bytes: 0,
            version: 0,
            status: Status::UnderConstruction,
            replicas: vec![],
        };

        let mut target_bucket = unsafe {
            self.inner_storage
                .get_unchecked(self.bucket_index(chunk_id))
                .lock()
                .unwrap()
        };

        target_bucket.insert(chunk_id, meta_info);

        chunk_id
    }

    pub fn get_chunks(
        &self,
        chunk_ids: &[i64],
        cluster_watcher: &ClusterWatcher,
    ) -> Vec<ChunkMetaInfo> {
        chunk_ids
            .iter()
            .filter_map(|chunk_id| unsafe {
                let b_index = self.bucket_index(*chunk_id);
                let bucket = self.inner_storage.get_unchecked(b_index).lock().unwrap();

                bucket
                    .get(chunk_id)
                    .and_then(|meta_info| match meta_info.status {
                        Status::Complete => {
                            let mut meta_info = meta_info.clone();
                            meta_info
                                .replicas
                                .retain(|rinfo| cluster_watcher.is_availabel(&rinfo.server_id));

                            (!meta_info.replicas.is_empty()).then_some(meta_info)
                        }
                        _ => None,
                    })
            })
            .collect()
    }

    pub fn next_id(&self) -> i64 {
        self.gen_id.next_id()
    }

    pub fn remove_chunk(&self, chunk_id: i64) {
        let mut target_bucket = unsafe {
            self.inner_storage
                .get_unchecked(self.bucket_index(chunk_id))
                .lock()
                .unwrap()
        };

        target_bucket.remove(&chunk_id);
    }

    pub fn check_empty(&self, chunk_id: &i64) {
        let mut target_bucket = unsafe {
            self.inner_storage
                .get_unchecked(self.bucket_index(*chunk_id))
                .lock()
                .unwrap()
        };

        let is_empty = if let Some(c) = target_bucket.get(chunk_id) {
            c.num_bytes == 0
        } else {
            false
        };

        if is_empty {
            target_bucket.remove(chunk_id);
        }
    }

    pub fn upload_complete(&self, chunk_id: i64, num_bytes: u32, _chunk_size: u32) {
        let mut target_bucket = unsafe {
            self.inner_storage
                .get_unchecked(self.bucket_index(chunk_id))
                .lock()
                .unwrap()
        };

        if let Some(chunk_meta) = target_bucket.get_mut(&chunk_id) {
            chunk_meta.num_bytes = num_bytes;
            chunk_meta.status = Status::Committed;
        }
    }

    pub fn replica_damage(
        &self,
        chunk_id: i64,
        replica_id: i64,
        server_id: u32,
    ) -> Option<(i64, u32)> {
        let mut target_bucket = unsafe {
            self.inner_storage
                .get_unchecked(self.bucket_index(chunk_id))
                .lock()
                .unwrap()
        };

        if let Some(chunk_meta) = target_bucket.get_mut(&chunk_id) {
            if let Some(index) = chunk_meta
                .replicas
                .iter()
                .position(|r| r.replica_id == replica_id && r.server_id == server_id)
            {
                let mut check_set = self.check_set.lock().unwrap();
                check_set.insert(chunk_meta.chunk_id);

                chunk_meta.replicas.remove(index);

                if chunk_meta.replicas.is_empty() {
                    chunk_meta.status = Status::UnderRecover;
                    return None;
                }

                return Some((
                    chunk_meta.replicas[0].replica_id,
                    chunk_meta.replicas[0].server_id,
                ));
            }
        }

        None
    }

    pub fn status_report(&self, report_info: StatusReportRequest) -> bool {
        let mut target_bucket = unsafe {
            self.inner_storage
                .get_unchecked(self.bucket_index(report_info.chunk_id))
                .lock()
                .unwrap()
        };

        match target_bucket.get_mut(&report_info.chunk_id) {
            Some(chunk_meta) => {
                if chunk_meta.replicas.len() >= NUM_REPLICA {
                    return false;
                }

                if report_info.is_update {
                    chunk_meta.version = report_info.version;
                }

                if chunk_meta.num_bytes != report_info.num_bytes {
                    return false;
                }

                if chunk_meta
                    .replicas
                    .iter()
                    .find(|&r| {
                        r.replica_id == report_info.replica_id
                            && r.server_id == report_info.server_id
                    })
                    .is_some()
                {
                    return false;
                }

                chunk_meta.replicas.push(ReplicaInfo {
                    server_id: report_info.server_id,
                    replica_id: report_info.replica_id,
                    status: Status::Complete,
                });

                chunk_meta.status = Status::Complete;
            }
            None => {
                let chunk_meta = ChunkMetaInfo {
                    chunk_id: report_info.chunk_id,
                    chunk_size: report_info.size,
                    num_bytes: report_info.num_bytes,
                    version: report_info.version,
                    status: Status::Complete,
                    replicas: vec![ReplicaInfo {
                        server_id: report_info.server_id,
                        replica_id: report_info.replica_id,
                        status: Status::Complete,
                    }],
                };

                target_bucket.insert(report_info.chunk_id, chunk_meta);
            }
        }

        true
    }

    pub fn bucket_index(&self, chunk_id: i64) -> usize {
        chunk_id as usize % self.inner_storage.len()
    }

    pub fn all_replicas(&self, chunk_ids: &[i64]) -> HashMap<u32, Vec<i64>> {
        let mut replica_map = HashMap::new();

        let mut bucket_map = HashMap::new();

        let bucket_size = self.inner_storage.len();
        for id in chunk_ids {
            bucket_map
                .entry(*id as usize % bucket_size)
                .or_insert_with(Vec::new)
                .push(*id);
        }

        for (index, ids) in bucket_map.into_iter() {
            let bucket = unsafe { self.inner_storage.get_unchecked(index).lock().unwrap() };

            for id in &ids {
                if let Some(chunk_info) = bucket.get(id) {
                    for replica_info in &chunk_info.replicas {
                        replica_map
                            .entry(replica_info.server_id)
                            .or_insert_with(Vec::new)
                            .push(replica_info.replica_id);
                    }
                }
            }
        }

        replica_map
    }
}

pub async fn check_recover_task(chunk_mgr: Arc<ChunkManager>, mut shutdown: Shutdown) {
    use tokio::time::{Duration, Instant};

    const CHECK_INTERVAL_TIME: Duration = Duration::from_secs(30 * 60);

    while shutdown.is_shutdown() {
        tokio::select! {
            _ = async {
                // TODO: check
                {
                    let mut check_map = chunk_mgr.check_set.lock().unwrap();

                    check_map.retain(|chunk_id| {
                        let mut target_bucket = unsafe {
                            chunk_mgr
                                .inner_storage
                                .get_unchecked(chunk_mgr.bucket_index(*chunk_id))
                                .lock()
                                .unwrap()
                        };

                        match target_bucket.get_mut(chunk_id) {
                            Some(chunk_info) => {
                                if chunk_info.replicas.len() == NUM_REPLICA {
                                    false
                                } else {
                                    if chunk_info.replicas.is_empty() {
                                        chunk_info.status = Status::Damage;
                                        false
                                    } else {
                                        true
                                    }
                                }
                            }
                            None => false,
                        }
                    });
                }

                let next_check_time = Instant::now() + CHECK_INTERVAL_TIME;
                tokio::time::sleep_until(next_check_time).await;
            } => (),
            _ = shutdown.recv() => {
                tracing::info!("master chunk recover task shutdown");
            }
        }
    }
}
