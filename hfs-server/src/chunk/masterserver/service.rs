use std::{net::SocketAddr, sync::Arc};

use hfs_proto::{
    chunk::{
        ChunkAddRequest, ChunkAddResponse, MasterChunk, ReplicaDamageRequest,
        ReplicaDamageResponse, ReplicaRecoverRequest, StatusReportRequest, StatusReportResponse,
        UploadCompleteRequest, UploadCompleteResponse,
    },
    cluster::ServiceType,
};
use volo_grpc::{Request, Response, Status};

use crate::{
    chunk::cfg::NUM_REPLICA, cluster::ClusterWatcher, grpc_client::chunk_client,
    shutdown::Shutdown, util::grpc::volo_status,
};

use super::{manager::check_recover_task, ChunkManager};

#[derive(Debug)]
pub struct ChunkMetaService {
    chunk_meta: Arc<ChunkManager>,
    cluster_watcher: Arc<ClusterWatcher>,
}

impl ChunkMetaService {
    pub fn new(chunk_meta: Arc<ChunkManager>, cluster_watcher: Arc<ClusterWatcher>) -> Self {
        ChunkMetaService {
            chunk_meta,
            cluster_watcher,
        }
    }

    pub fn backgroud_task(&self, shutdown: Shutdown) {
        tokio::spawn(check_recover_task(self.chunk_meta.clone(), shutdown));
    }
}

impl MasterChunk for ChunkMetaService {
    async fn status_report(
        &self,
        req: Request<StatusReportRequest>,
    ) -> Result<Response<StatusReportResponse>, Status> {
        let req_params = req.into_inner();

        tracing::info!("[ChunkManager/status_report] {:?}", req_params);

        if !self.chunk_meta.status_report(req_params) {
            return Err(volo_status("Replica status report error"));
        }

        Ok(Response::new(StatusReportResponse {}))
    }

    async fn upload_complete(
        &self,
        req: Request<UploadCompleteRequest>,
    ) -> Result<Response<UploadCompleteResponse>, Status> {
        let req_params = req.into_inner();

        tracing::info!("[ChunkManager/upload_complete] {:?}", req_params);

        if req_params.status {
            self.chunk_meta.upload_complete(
                req_params.chunk_id,
                req_params.num_bytes,
                req_params.size,
            );
        } else {
            self.chunk_meta.check_empty(&req_params.chunk_id);
        }

        Ok(Response::new(UploadCompleteResponse {}))
    }

    async fn replica_damage(
        &self,
        req: Request<ReplicaDamageRequest>,
    ) -> Result<Response<ReplicaDamageResponse>, Status> {
        let req_params = req.into_inner();

        tracing::info!("[ChunkManager/replica_damage] {:?}", req_params);

        if let Some((replica_id, server_id)) = self.chunk_meta.replica_damage(
            req_params.chunk_id,
            req_params.replica_id,
            req_params.server_id,
        ) {
            tracing::info!(
                target_id = replica_id,
                target_server_id = server_id,
                "[ChunkManager/replica_damage] replica recover: "
            );

            chunk_replica_recover(
                &self.cluster_watcher,
                req_params.replica_id,
                replica_id,
                req_params.server_id,
                server_id,
            )
            .await;
        } else {
            tracing::info!(
                chunk_id = req_params.chunk_id,
                "[ChunkManager/replica_damage] Block cannot be recovered, no replica available: "
            );
        }

        Ok(Response::new(ReplicaDamageResponse {}))
    }

    async fn chunk_add(
        &self,
        _req: Request<ChunkAddRequest>,
    ) -> Result<Response<ChunkAddResponse>, Status> {
        let chunk_id = self.chunk_meta.add_chunk();
        let server_ids = self.cluster_watcher.next_replica_server_id(NUM_REPLICA);

        Ok(Response::new(ChunkAddResponse {
            chunk_id,
            server_id: server_ids,
        }))
    }
}

pub async fn chunk_replica_recover(
    cluster_watcher: &ClusterWatcher,
    src_id: i64,
    target_id: i64,
    src_server: u32,
    target_server: u32,
) {
    let addr: SocketAddr = match cluster_watcher.service_socket_addr(&src_server, &ServiceType::Rpc)
    {
        Ok(addr) => addr.parse().unwrap(),
        Err(err) => {
            tracing::error!(error = ?err, "chunk replica recover error");
            return;
        }
    };

    let chunk_client = chunk_client(addr);

    if let Err(err) = chunk_client
        .replica_recover(ReplicaRecoverRequest {
            src_id,
            target_id,
            server_id: target_server,
        })
        .await
    {
        tracing::error!(recover_error = ?err, "server id: {}, replica id: {}", src_server, src_id);
    }
}
