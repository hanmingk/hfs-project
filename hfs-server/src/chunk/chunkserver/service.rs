use std::{net::SocketAddr, sync::Arc};

use hfs_proto::{
    chunk::{
        Chunk, ReplicaAddRequest, ReplicaAddResponse, ReplicaRecoverRequest,
        ReplicaRecoverResponse, ReplicaRemoveRequest, ReplicaRemoveResponse,
    },
    cluster::{ServiceAddrRequest, ServiceType},
};
use tokio::fs;
use volo_grpc::{Request, Response, Status};

use crate::{
    chunk::chunkserver::replica::{data_dir, meta_dir, ReplicaMetaInfo},
    grpc_client::CLUSTER_CLIENT,
    transport::chunkserver::{connect_copy, receive_copy},
    util::grpc::volo_status,
};

use super::manager::ReplicaManager;

#[derive(Debug)]
pub struct ReplicaService {
    replica_mgr: Arc<ReplicaManager>,
}

impl ReplicaService {
    pub fn new(replica_manager: Arc<ReplicaManager>) -> ReplicaService {
        ReplicaService {
            replica_mgr: replica_manager,
        }
    }
}

impl Chunk for ReplicaService {
    async fn replica_recover(
        &self,
        req: Request<ReplicaRecoverRequest>,
    ) -> Result<Response<ReplicaRecoverResponse>, Status> {
        let req_params = req.into_inner();

        tracing::info!("replica_recover: {:?}", req_params);

        let addr_resp = CLUSTER_CLIENT
            .service_addr(ServiceAddrRequest {
                server_id: req_params.server_id,
                service_type: ServiceType::Tcp,
            })
            .await?
            .into_inner();

        let replica_mgr = self.replica_mgr.clone();
        tokio::spawn(async move {
            let addr: SocketAddr = addr_resp.socket_addr.parse().unwrap();

            replica_mgr.update_replica(&req_params.src_id);
            match connect_copy(addr, req_params.target_id).await {
                Ok(stream) => {
                    if let Err(err) = receive_copy(stream, req_params.src_id, &replica_mgr).await {
                        tracing::error!(replica_id = req_params.src_id, cause = ?err, "Replica recovery error waiting for next recoveri")
                    };
                }
                Err(err) => {
                    tracing::error!(replica_id = req_params.src_id, cause = ?err, "Replica recovery error waiting for next recoveri")
                }
            };
        });

        Ok(Response::new(ReplicaRecoverResponse {}))
    }

    async fn replica_add(
        &self,
        req: Request<ReplicaAddRequest>,
    ) -> Result<Response<ReplicaAddResponse>, Status> {
        let req_params = req.into_inner();

        let meta_info = match ReplicaMetaInfo::new(
            &self.replica_mgr.base_dir,
            req_params.chunk_id,
            self.replica_mgr.next_replica_id(),
        )
        .await
        {
            Ok(meta_info) => meta_info,
            Err(err) => {
                tracing::error!(cause = ?err, "replica add error");
                return Err(volo_status("replica add error"));
            }
        };

        let replica_id = meta_info.replica_id;

        self.replica_mgr.add_meta(meta_info);

        Ok(Response::new(ReplicaAddResponse { replica_id }))
    }

    async fn replica_remove(
        &self,
        req: Request<ReplicaRemoveRequest>,
    ) -> Result<Response<ReplicaRemoveResponse>, Status> {
        let req_params = req.into_inner();

        tracing::info!("replica_remove: {:?}", req_params);

        for id in self.replica_mgr.remove_replicas(&req_params.replica_ids) {
            let _ = fs::remove_file(data_dir(&self.replica_mgr.base_dir, id)).await;
            let _ = fs::remove_file(meta_dir(&self.replica_mgr.base_dir, id)).await;
        }

        Ok(Response::new(ReplicaRemoveResponse {}))
    }

    // async fn replica_copy(
    //     &self,
    //     req: Request<ReplicaCopyRequest>,
    // ) -> Result<Response<ReplicaCopyResponse>, Status> {
    //     let req_params = req.into_inner();
    //     let request = ServiceAddrRequest {
    //         server_id: req_params.server_id,
    //         service_type: ServiceType::Tcp,
    //     };
    //     let addr_resp = CLUSTER_CLIENT.service_addr(request).await?.into_inner();

    //     let socket_addr: SocketAddr = addr_resp.socket_addr.parse().unwrap();
    //     let mut socket = connect_copy(socket_addr, req_params.target_id).await?;

    //     // send replica packet
    //     let meta_info = self
    //         .replica_mgr
    //         .get_replica_meta(&req_params.src_id)
    //         .ok_or(volo_status("Unknown src id"))?;

    //     let mut replica_file = ReplicaFile::from(meta_info).await?;
    //     let replica_id = replica_file.replica_id();
    //     let chunk_id = replica_file.chunk_id();
    //     let mut packets = replica_file.packets().await;
    //     let crc = CRC32::new();
    //     while let Some(mut packet) = packets.next().await {
    //         if !packet.check_pkt(&crc) {
    //             self.replica_mgr
    //                 .update_status(&replica_id, chunk::Status::UnderRecover);

    //             MASTER_CHUNK_CLIENT
    //                 .replica_damage(ReplicaDamageRequest {
    //                     chunk_id,
    //                     replica_id,
    //                 })
    //                 .await?;

    //             return Err(volo_status("Copied replica demage"));
    //         }

    //         packet.write_to(&mut socket).await?;
    //     }

    //     let history_info = replica_file
    //         .history_info()
    //         .await
    //         .map_err(|_| volo_status("Get history info error"))?;

    //     history_to(&mut socket, history_info)
    //         .await
    //         .map_err(|_| volo_status("history info send error"))?;

    //     Ok(Response::new(ReplicaCopyResponse {}))
    // }
}
