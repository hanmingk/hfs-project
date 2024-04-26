pub mod service;

mod copy;
mod download;
mod upload;

mod task {
    pub use super::copy::copy_task;
    pub use super::download::download_task;
    pub use super::upload::upload_task;
}

pub use copy::{connect_copy, receive_copy};
pub use download::connect_download;
pub use upload::connect_upload;

use crate::{chunk::chunkserver::manager::ReplicaManager, grpc_client::MASTER_CHUNK_CLIENT};
use hfs_proto::chunk::{ReplicaDamageRequest, Status};

pub async fn report_demage(replica_mgr: &ReplicaManager, chunk_id: i64, replica_id: i64) {
    tracing::info!(
        replica_id,
        "Replica is damaged and waiting to be restored: "
    );

    replica_mgr.update_status(&replica_id, Status::UnderRecover);

    let _ = MASTER_CHUNK_CLIENT
        .replica_damage(ReplicaDamageRequest {
            chunk_id,
            replica_id,
            server_id: replica_mgr.server_id,
        })
        .await;
}
