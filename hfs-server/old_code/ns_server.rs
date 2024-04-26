// use std::{result::Result, sync::Arc};

// use hfs_proto::{
//     master::{
//         BasicWithFileInfo, FileSystemService, LeaseFileId, LeaseIdTokenPath, PathOpcode,
//         TokenPathOpcode,
//     },
//     shared::{BasicResponse, Status},
// };

// use volo_grpc::{Request, Response, Status};

// use crate::{
//     auth::AuthManager,
//     chunk::ChunkManager,
//     cluster::ClusterGuard,
//     lease::{LeaseItem, Lessor},
//     namespace::node::NodeType,
//     util::grpc::result_to_response,
// };

// use super::file_system::{FileSystem, NODE_LEASE_PREFIX};

// #[derive(Debug)]
// pub struct FileNameSpace {
//     pub file_system: Arc<FileSystem>,
//     pub chunk_manager: Arc<ChunkManager>,
//     pub cluster: Arc<ClusterGuard>,
//     pub auth: Arc<AuthManager>,
//     pub lessor: Arc<Lessor>,
// }

// impl FileSystemService for FileNameSpace {
//     async fn path_operations(
//         &self,
//         req: Request<TokenPathOpcode>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.into_inner();
//         tracing::info!("path_operations: {:?}", params);
//         match params.opcode {
//             PathOpcode::CreateFile => {
//                 let res = self.file_system.create_file_or_directory(
//                     &params.token,
//                     &self.auth,
//                     (params.path.as_ref()).into(),
//                     NodeType::FileNode,
//                     None,
//                 );
//                 Ok(result_to_response(res))
//             }
//             PathOpcode::CreateDirectory => {
//                 let res = self.file_system.create_file_or_directory(
//                     &params.token,
//                     &self.auth,
//                     (params.path.as_ref()).into(),
//                     NodeType::DirectoryNode,
//                     None,
//                 );
//                 Ok(result_to_response(res))
//             }
//             PathOpcode::RemoveFile | PathOpcode::RemoveDirectory => {
//                 let res = self.file_system.remove_file_or_directory(
//                     &params.token,
//                     &self.auth,
//                     (params.path.as_ref()).into(),
//                 );
//                 Ok(result_to_response(res))
//             }
//         }
//     }

//     async fn open_file(
//         &self,
//         req: Request<LeaseIdTokenPath>,
//     ) -> Result<Response<BasicWithFileInfo>, volo_grpc::Status> {
//         let params = req.into_inner();
//         if !self.lessor.is_live(&params.lease_id) {
//             return Ok(Response::new(BasicWithFileInfo {
//                 status: Status::Error,
//                 msg: "Unknown leass".into(),
//                 file_info: None,
//             }));
//         }

//         let file_id = match self.file_system.load_file_to_cache(
//             &params.token,
//             &self.auth,
//             (params.path.as_ref()).into(),
//         ) {
//             Ok(id) => id,
//             Err(err) => {
//                 return Ok(Response::new(BasicWithFileInfo {
//                     status: Status::Error,
//                     msg: err.into(),
//                     file_info: None,
//                 }))
//             }
//         };

//         if let Err(err) = self
//             .lessor
//             .lock(params.lease_id, LeaseItem::new(&NODE_LEASE_PREFIX, file_id))
//             .await
//         {
//             self.file_system.delete_file_cache(&file_id);
//             return Ok(Response::new(BasicWithFileInfo {
//                 status: Status::Error,
//                 msg: err.into(),
//                 file_info: None,
//             }));
//         }

//         let maybe_file_info = self.file_system.get_file_info(
//             &self.auth,
//             &self.chunk_manager,
//             &self.cluster,
//             &file_id,
//         );
//         match maybe_file_info {
//             Some(file_info) => Ok(Response::new(BasicWithFileInfo {
//                 status: Status::Ok,
//                 msg: "Open file success!!".into(),
//                 file_info: Some(file_info),
//             })),
//             None => {
//                 self.lessor.unlock(
//                     &params.lease_id,
//                     &LeaseItem::new(&NODE_LEASE_PREFIX, file_id),
//                 );
//                 Ok(Response::new(BasicWithFileInfo {
//                     status: Status::Error,
//                     msg: "Open file faild!!".into(),
//                     file_info: None,
//                 }))
//             }
//         }
//     }

//     async fn close_file(
//         &self,
//         req: Request<LeaseFileId>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.into_inner();

//         self.file_system.delete_file_cache(&params.file_id);
//         self.lessor.unlock(
//             &params.lease_id,
//             &LeaseItem::new(&NODE_LEASE_PREFIX, params.file_id),
//         );
//         Ok(Response::new(BasicResponse {
//             status: Status::Ok,
//             msg: "Close file success!!".into(),
//         }))
//     }
// }
