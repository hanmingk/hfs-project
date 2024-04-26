use std::sync::Arc;

use hfs_proto::lease::{
    Lease, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, ResourceLockRequest, ResourceLockResponse,
    ResourceUnlockRequest, ResourceUnlockResponse,
};
use tokio::sync::mpsc;
use volo_grpc::{Request, Response, Status};

use crate::shutdown::Shutdown;

use super::{lessor::check_lease_alive_tasks, Lessor};

#[derive(Debug)]
pub struct LeaseService {
    lessor: Arc<Lessor>,
}

impl LeaseService {
    pub fn new(lessor: Arc<Lessor>) -> Self {
        LeaseService { lessor }
    }

    pub fn backgroud_task(&self, shutdown: Shutdown, shutdown_complete: mpsc::Sender<()>) {
        tokio::spawn(check_lease_alive_tasks(
            self.lessor.clone(),
            shutdown,
            shutdown_complete,
        ));
    }
}

impl Lease for LeaseService {
    async fn lease_grant(
        &self,
        req: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let req_params = req.into_inner();
        let lease_id = self.lessor.lease_grant(req_params.ttl);

        Ok(Response::new(LeaseGrantResponse {
            id: lease_id,
            ttl: req_params.ttl,
        }))
    }

    async fn lease_revoke(
        &self,
        req: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let req_params = req.into_inner();
        self.lessor.lease_revoke(&req_params.id);
        Ok(Response::new(LeaseRevokeResponse {}))
    }

    async fn lease_keep_alive(
        &self,
        req: Request<LeaseKeepAliveRequest>,
    ) -> Result<Response<LeaseKeepAliveResponse>, Status> {
        let req_params = req.into_inner();
        if !self.lessor.update_lease(&req_params.id, req_params.ttl) {
            return Err(Status::new(volo_grpc::Code::Unknown, "Unknown lease"));
        }

        Ok(Response::new(LeaseKeepAliveResponse {
            ttl: req_params.ttl,
        }))
    }

    async fn resource_lock(
        &self,
        req: Request<ResourceLockRequest>,
    ) -> Result<Response<ResourceLockResponse>, Status> {
        let req_params = req.into_inner();
        self.lessor
            .resource_lock(req_params.lease_id, req_params.rsrc_key.into())
            .await?;

        Ok(Response::new(ResourceLockResponse {}))
    }

    async fn resource_unlock(
        &self,
        req: Request<ResourceUnlockRequest>,
    ) -> Result<Response<ResourceUnlockResponse>, Status> {
        let req_params = req.into_inner();
        self.lessor
            .resource_unlock(&req_params.lease_id, &req_params.rsrc_key.into());

        Ok(Response::new(ResourceUnlockResponse {}))
    }
}
