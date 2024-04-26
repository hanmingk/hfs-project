use std::{net::Ipv4Addr, sync::Arc};

use ahash::AHashMap;
use hfs_proto::cluster::{
    Cluster, HeartbeatRequest, HeartbeatResponse, MemberAddRequest, MemberAddResponse,
    MemberRemoveRequest, MemberRemoveResponse, ServiceAddrRequest, ServiceAddrResponse,
};
use volo::FastStr;
use volo_grpc::{Request, Response, Status};

use crate::shutdown::Shutdown;

use super::{
    server::{self, ChunkServer},
    watcher::{check_heartbeat_task, ClusterWatcher},
};

#[derive(Debug)]
pub struct ClusterService {
    watcher: Arc<ClusterWatcher>,
}

impl ClusterService {
    pub fn new(watcher: Arc<ClusterWatcher>) -> Self {
        ClusterService { watcher }
    }

    pub fn backgroud_task(&self, shudown: Shutdown) {
        tokio::spawn(check_heartbeat_task(self.watcher.clone(), shudown));
    }
}

impl Cluster for ClusterService {
    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.watcher.heartbeat(req.into_inner())?;
        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn member_add(
        &self,
        req: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        let req_param = req.into_inner();
        let Ok(addr) = (&req_param.addr).parse::<Ipv4Addr>() else {
            return Err(Status::new(volo_grpc::Code::Unknown, "Error ipv4 addr"));
        };

        let mut ports = AHashMap::new();
        for port in req_param.service_ports {
            ports.insert(port.service_type, port.port as u16);
        }
        let chunk_server = ChunkServer {
            server_id: req_param.server_id,
            addr: addr.octets(),
            service_ports: ports.clone(),
            status: server::ServerStatus::Online,
        };

        let server_id = self.watcher.member_add(chunk_server)?;

        if server_id == 0 {
            tracing::info!(server_id, ip = ?addr, service_ports = ?ports, "new chunk server joins the cluster:");
        } else {
            tracing::info!(server_id, ip = ?addr, service_ports = ?ports, "chunk server reconnects to the cluster:");
        }

        Ok(Response::new(MemberAddResponse { server_id }))
    }

    async fn member_remove(
        &self,
        req: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        let req_param = req.into_inner();
        self.watcher.member_remove(req_param.server_id);
        Ok(Response::new(MemberRemoveResponse {}))
    }

    async fn service_addr(
        &self,
        req: Request<ServiceAddrRequest>,
    ) -> Result<Response<ServiceAddrResponse>, Status> {
        let req_param = req.into_inner();
        let socket_addr = self
            .watcher
            .service_socket_addr(&req_param.server_id, &req_param.service_type)?;

        Ok(Response::new(ServiceAddrResponse {
            socket_addr: FastStr::from_string(socket_addr),
        }))
    }
}
