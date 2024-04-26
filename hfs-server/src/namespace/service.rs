use std::sync::Arc;

use hfs_proto::{
    chunk::ReplicaRemoveRequest,
    cluster::ServiceType,
    master::{
        CloseFileRequest, CloseFileResponse, ContainsPathRequest, ContainsPathResponse,
        CreateDirectoryRequest, CreateDirectoryResponse, CreateFileRequest, CreateFileResponse,
        ListDirectoryRequest, ListDirectoryResponse, MovePathRequest, MovePathResponse,
        OpenFileRequest, OpenFileResponse, RemoveDirectoryRequest, RemoveDirectoryResponse,
        RemoveFileRequest, RemoveFileResponse,
    },
};
use volo_grpc::{Request, Response, Status};

use crate::{
    auth::AuthManager, chunk::masterserver::ChunkManager, cluster::ClusterWatcher,
    grpc_client::chunk_client, lease::Lessor, namespace::NodeType, util::grpc::volo_status,
};

use super::{FileSystem, HFSPath};

#[derive(Debug)]
pub struct FileSystemService {
    pub file_system: Arc<FileSystem>,
    pub chunk_manager: Arc<ChunkManager>,
    pub cluster: Arc<ClusterWatcher>,
    pub auth: Arc<AuthManager>,
    pub lessor: Arc<Lessor>,
}

impl hfs_proto::master::FileSystem for FileSystemService {
    async fn create_file(
        &self,
        req: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let req_param = req.into_inner();
        self.file_system.create_file_or_directory(
            &req_param.token,
            &self.auth,
            (req_param.file_path.as_bytes()).into(),
            NodeType::FileNode,
            None,
        )?;
        Ok(Response::new(CreateFileResponse {}))
    }

    async fn remove_file(
        &self,
        req: Request<RemoveFileRequest>,
    ) -> Result<Response<RemoveFileResponse>, Status> {
        let req_param = req.into_inner();
        let all_chunks = self.file_system.remove_file_or_directory(
            &req_param.token,
            &self.auth,
            (req_param.file_path.as_bytes()).into(),
        )?;

        tracing::info!(
            "[FileSystem/remove_file] {} {:?}",
            req_param.file_path,
            all_chunks
        );

        tokio::spawn(remove_chunk(
            self.chunk_manager.clone(),
            self.cluster.clone(),
            all_chunks,
        ));

        Ok(Response::new(RemoveFileResponse {}))
    }

    async fn create_directory(
        &self,
        req: Request<CreateDirectoryRequest>,
    ) -> Result<Response<CreateDirectoryResponse>, Status> {
        let req_param = req.into_inner();

        if req_param.parent {
            let hfs_path = HFSPath::from(req_param.dir_path.as_bytes());
            let mut components = hfs_path.components();
            components.next(); // skip root path

            let mut exist_path = vec![];

            while let Some(comp) = components.next() {
                exist_path.push(b'/');
                exist_path.extend(comp);

                let hfs_path = HFSPath::from(&exist_path);
                match self.file_system.contains_path(&hfs_path) {
                    Ok(false) => {
                        return Err(volo_status("no such file or directory"));
                    }
                    Ok(true) => {}
                    Err(_) => {
                        self.file_system.create_file_or_directory(
                            &req_param.token,
                            &self.auth,
                            hfs_path,
                            NodeType::DirectoryNode,
                            None,
                        )?;
                    }
                }
            }
        } else {
            self.file_system.create_file_or_directory(
                &req_param.token,
                &self.auth,
                (req_param.dir_path.as_bytes()).into(),
                NodeType::DirectoryNode,
                None,
            )?;
        }

        Ok(Response::new(CreateDirectoryResponse {}))
    }

    async fn remove_directory(
        &self,
        req: Request<RemoveDirectoryRequest>,
    ) -> Result<Response<RemoveDirectoryResponse>, Status> {
        let req_param = req.into_inner();

        self.file_system.remove_file_or_directory(
            &req_param.token,
            &self.auth,
            (req_param.dir_path.as_bytes()).into(),
        )?;

        Ok(Response::new(RemoveDirectoryResponse {}))
    }

    async fn move_path(
        &self,
        req: Request<MovePathRequest>,
    ) -> Result<Response<MovePathResponse>, Status> {
        let req_param = req.into_inner();
        self.file_system.move_path(
            &req_param.token,
            &self.auth,
            req_param.src_path.as_bytes().into(),
            req_param.target_path.as_bytes().into(),
        )?;

        Ok(Response::new(MovePathResponse {}))
    }

    async fn open_file(
        &self,
        req: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        let req_param = req.into_inner();
        // if !self.lessor.contains_lease(&req_param.lease_id) {
        //     return Err(Status::new(volo_grpc::Code::Unknown, "Unknown Lease"));
        // }

        let file_id = self.file_system.load_file_to_cache(
            &req_param.token,
            &self.auth,
            (req_param.file_path.as_bytes()).into(),
        )?;

        // if let Err(err) = self
        //     .lessor
        //     .resource_lock(
        //         req_param.lease_id,
        //         LeaseItem::new(NODE_LEASE_PREFIX, file_id),
        //     )
        //     .await
        // {
        //     self.file_system.delete_file_cache(&file_id);
        //     return Err(err.into());
        // }

        let Some(file_info) = self.file_system.get_file_info(
            &self.auth,
            &self.chunk_manager,
            &self.cluster,
            &file_id,
        ) else {
            // self.lessor.resource_unlock(
            //     &req_param.lease_id,
            //     &LeaseItem::new(NODE_LEASE_PREFIX, file_id),
            // );
            self.file_system.delete_file_cache(&file_id);
            return Err(Status::new(
                volo_grpc::Code::Unknown,
                "File data is incomplete",
            ));
        };

        Ok(Response::new(OpenFileResponse {
            meta_info: Some(file_info),
        }))
    }

    async fn close_file(
        &self,
        req: Request<CloseFileRequest>,
    ) -> Result<Response<CloseFileResponse>, Status> {
        let req_param = req.into_inner();

        let chunk_ids = req_param
            .chunk_ids
            .into_iter()
            .filter(|id| self.chunk_manager.contains_id(*id))
            .collect::<Vec<i64>>();

        self.file_system
            .file_cache_update(&req_param.file_id, chunk_ids);

        self.file_system.delete_file_cache(&req_param.file_id);
        // self.lessor.resource_unlock(
        //     &req_param.lease_id,
        //     &LeaseItem::new(NODE_LEASE_PREFIX, req_param.file_id),
        // );

        Ok(Response::new(CloseFileResponse {}))
    }

    async fn list_directory(
        &self,
        req: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let req_param = req.into_inner();

        let list_items = self
            .file_system
            .list_directory(&self.auth, req_param.path.as_bytes().into())?;

        Ok(Response::new(ListDirectoryResponse { items: list_items }))
    }

    async fn contains_path(
        &self,
        req: Request<ContainsPathRequest>,
    ) -> Result<Response<ContainsPathResponse>, Status> {
        let req_param = req.into_inner();

        let is_dir = self
            .file_system
            .contains_path(&req_param.path.as_bytes().into())?;

        Ok(Response::new(ContainsPathResponse { is_dir }))
    }
}

async fn remove_chunk(
    chunk_manager: Arc<ChunkManager>,
    cluster: Arc<ClusterWatcher>,
    chunks: Vec<i64>,
) {
    for (server_id, replicas) in chunk_manager.all_replicas(&chunks) {
        if let Ok(addr) = cluster.service_socket_addr(&server_id, &ServiceType::Rpc) {
            let _ = chunk_client(addr.parse().unwrap())
                .replica_remove(ReplicaRemoveRequest {
                    replica_ids: replicas,
                })
                .await;
        }
    }
}
