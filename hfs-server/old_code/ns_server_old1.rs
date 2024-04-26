// use std::{result::Result, sync::Arc};

// use faststr::FastStr;
// use hfs_proto::{
//     master::{
//         AuthOpcode, BasicWithFileInfo, BasicWithToken, FileInfo, FileSystemService, LeaseFileId,
//         LeaseIdTokenPath, PathOpcode, TokenNameOtherStrOpcode, TokenPathOpcode,
//     },
//     shared::{BasicResponse, Status},
// };
// use volo_grpc::{Request, Response};

// use crate::{
//     auth::{AuthError, AuthManager, Permissions, PermissionsInfo},
//     chunk::ChunkManager,
//     cluster::ClusterGuard,
//     lease::{LeaseItem, Lessor},
//     namespace::{
//         node::{Node, NodeInfo, NodeType},
//         path::HFSPath,
//     },
//     util::grpc::result_to_response,
// };

// use super::{
//     error::HFSIoError,
//     file_system::{FileSystem, NODE_LEASE_PREFIX},
// };

// #[derive(Debug)]
// pub struct FileNameSpace {
//     file_system: Arc<FileSystem>,
//     chunk_manager: Arc<ChunkManager>,
//     cluster: Arc<ClusterGuard>,
//     auth: Arc<AuthManager>,
//     lessor: Arc<Lessor>,
// }

// impl FileNameSpace {
//     pub fn new(
//         file_system: Arc<FileSystem>,
//         chunk_manager: Arc<ChunkManager>,
//         cluster: Arc<ClusterGuard>,
//         auth: Arc<AuthManager>,
//         lessor: Arc<Lessor>,
//     ) -> FileNameSpace {
//         FileNameSpace {
//             file_system,
//             chunk_manager,
//             cluster,
//             auth,
//             lessor,
//         }
//     }

//     fn create_file_or_directory(
//         &self,
//         auth_token: &str,
//         src: HFSPath,
//         node_type: NodeType,
//         permissions: Option<PermissionsInfo>,
//     ) -> Result<(), HFSIoError> {
//         let (user_id, _) = self.auth_token_to_id(auth_token)?;
//         let parent_path = src.parent_path().ok_or(HFSIoError::NoSuchFileOrDirectory)?;
//         let mut fs_directory = self.file_system.fs_directory.lock().unwrap();
//         let res = match fs_directory.get_node_mut(&parent_path) {
//             Some(parent_node) => {
//                 match self
//                     .auth
//                     .verify_permissions(user_id, &parent_node.permissions_info)
//                 {
//                     Permissions::WriteOnly | Permissions::ReadWithWrite => {
//                         let node_id = self.file_system.next_node_id();
//                         let name = src
//                             .last_path_name()
//                             .ok_or(HFSIoError::NoSuchFileOrDirectory)?;
//                         let new_node = match permissions {
//                             Some(per) => {
//                                 Node::with_permissions(node_id, name.to_vec(), node_type, per)
//                             }
//                             None => Node::new(node_id, name.to_vec(), node_type),
//                         };
//                         match parent_node.add_child(new_node) {
//                             Some(_) => Err(HFSIoError::ExistsFileOrDirectory),
//                             None => Ok(()),
//                         }
//                     }
//                     Permissions::NoPermissions | Permissions::ReadOnly => {
//                         Err(HFSIoError::NoPermissions)
//                     }
//                 }
//             }
//             None => Err(HFSIoError::NoSuchFileOrDirectory),
//         };
//         if res.is_ok() {
//             fs_directory.count += 1;
//         }
//         res
//     }

//     fn remove_file_or_directory(
//         &self,
//         auth_token: &str,
//         src: HFSPath,
//         _node_type: NodeType,
//     ) -> Result<(), HFSIoError> {
//         let (user_id, _) = self.auth_token_to_id(auth_token)?;
//         let parent_path = src.parent_path().ok_or(HFSIoError::NoSuchFileOrDirectory)?;
//         let mut fs_directory = self.file_system.fs_directory.lock().unwrap();
//         let res = match fs_directory.get_node_mut(&parent_path) {
//             Some(parent_node) => {
//                 match self
//                     .auth
//                     .verify_permissions(user_id, &parent_node.permissions_info)
//                 {
//                     Permissions::WriteOnly | Permissions::ReadWithWrite => {
//                         let name = src
//                             .last_path_name()
//                             .ok_or(HFSIoError::NoSuchFileOrDirectory)?;
//                         match parent_node.remove_child(name) {
//                             Some(_) => Ok(()),
//                             None => Err(HFSIoError::NoSuchFileOrDirectory),
//                         }
//                     }
//                     Permissions::NoPermissions | Permissions::ReadOnly => {
//                         Err(HFSIoError::NoPermissions)
//                     }
//                 }
//             }
//             None => Err(HFSIoError::NoSuchFileOrDirectory),
//         };
//         if res.is_ok() {
//             fs_directory.count -= 1;
//         }
//         res
//     }

//     fn open_file(&self, auth_token: &str, src: HFSPath) -> Result<FileInfo, HFSIoError> {
//         let (user_id, group_id) = self.auth_token_to_id(auth_token)?;

//         let mut file_info = self.get_file_info(&src, user_id)?;
//         let user_name = self
//             .auth
//             .get_user_name(auth_token)
//             .ok_or(HFSIoError::UnSignIn)?;
//         file_info.owner = FastStr::from_string(user_name);
//         if let Some(group_name) = self.auth.get_group_name(&group_id) {
//             file_info.group = FastStr::from_string(group_name);
//         }
//         Ok(file_info)
//     }

//     fn get_file_info(&self, path: &HFSPath, user_id: u16) -> Result<FileInfo, HFSIoError> {
//         let fs_directory = self.file_system.fs_directory.lock().unwrap();
//         let node = fs_directory
//             .get_node(path)
//             .ok_or(HFSIoError::NoSuchFileOrDirectory)?;
//         if let Permissions::NoPermissions = self
//             .auth
//             .verify_permissions(user_id, &node.permissions_info)
//         {
//             return Err(HFSIoError::NoPermissions);
//         }
//         if let NodeInfo::File(file_info) = &node.node_info {
//             let mut buckets_lock_vec = Vec::with_capacity(self.chunk_manager.bucket_size());

//             let mut file_length = 0;
//             let mut file_info_chunks = Vec::with_capacity(file_info.chunks.len());
//             let chunk_servers = self.cluster.servers.lock().unwrap();
//             for id in file_info.chunks.iter() {
//                 let index = self.chunk_manager.bucket_index(id);
//                 if let None = buckets_lock_vec.get(index) {
//                     buckets_lock_vec.insert(
//                         index,
//                         self.chunk_manager.inner_storage[index].lock().unwrap(),
//                     );
//                 }
//                 match buckets_lock_vec[index].get(id) {
//                     Some(chunk_meta) => {
//                         let chunk = chunk_meta.to_rpc_chunk(&chunk_servers);
//                         if chunk.replicas.len() == 0 {
//                             return Err(HFSIoError::FileBlockCorruption);
//                         }
//                         file_length += chunk.length as u64;
//                         file_info_chunks.push(chunk);
//                     }
//                     None => return Err(HFSIoError::NoSuchFileOrDirectory),
//                 }
//             }

//             return Ok(FileInfo {
//                 id: node.id.0,
//                 name: unsafe { FastStr::from_vec_u8_unchecked(node.name.clone()) },
//                 group: FastStr::empty(),
//                 owner: FastStr::empty(),
//                 length: file_length,
//                 mdfc_time: node.mdfc_time,
//                 acs_time: node.acs_time,
//                 chunks: file_info_chunks,
//             });
//         }

//         Err(HFSIoError::NoSuchFileOrDirectory)
//     }

//     fn auth_token_to_id(&self, auth_token: &str) -> Result<(u16, u16), HFSIoError> {
//         let id = self
//             .auth
//             .verify_identity(auth_token)
//             .ok_or(HFSIoError::UnSignIn)?;
//         Ok(id)
//     }
// }

// impl FileSystemService for FileNameSpace {
//     async fn path_operations(
//         &self,
//         req: Request<TokenPathOpcode>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.get_ref();
//         tracing::info!("path_operations: {:?}", params);
//         match params.opcode {
//             PathOpcode::CreateFile => {
//                 let res = self.create_file_or_directory(
//                     &params.token,
//                     (params.path.as_ref()).into(),
//                     NodeType::File,
//                     None,
//                 );
//                 Ok(result_to_response(res))
//             }
//             PathOpcode::CreateDirectory => {
//                 let res = self.create_file_or_directory(
//                     &params.token,
//                     (params.path.as_ref()).into(),
//                     NodeType::Directory,
//                     None,
//                 );
//                 Ok(result_to_response(res))
//             }
//             PathOpcode::RemoveFile | PathOpcode::RemoveDirectory => {
//                 let res = self.remove_file_or_directory(
//                     &params.token,
//                     (params.path.as_ref()).into(),
//                     NodeType::File,
//                 );
//                 Ok(result_to_response(res))
//             }
//         }
//     }

//     async fn auth_operations(
//         &self,
//         req: Request<TokenNameOtherStrOpcode>,
//     ) -> Result<Response<BasicWithToken>, volo_grpc::Status> {
//         let params = req.get_ref();
//         tracing::info!("auth_operations: {:?}", params);
//         match params.opcode {
//             AuthOpcode::SignIn => {
//                 let res = self.auth.sign_in(&params.name, &params.other_str);
//                 Ok(Response::new(BasicWithToken {
//                     status: match res {
//                         Some(_) => Status::Ok,
//                         None => Status::Error,
//                     },
//                     msg: match res {
//                         Some(_) => "Sign in success!!".into(),
//                         None => "Sing in fail".into(),
//                     },
//                     token: res,
//                 }))
//             }
//             AuthOpcode::CreateUser => {
//                 let res = self
//                     .auth
//                     .create_user(&params.token, &params.name, &params.other_str);
//                 let resp = match res {
//                     Ok(id) => {
//                         let user_home = format!("/home/{}", &params.name);
//                         let user_home_path = HFSPath::from(user_home.as_bytes());
//                         let permissions: PermissionsInfo = PermissionsInfo::with_id(id.0, id.1);
//                         let _ = self.create_file_or_directory(
//                             &params.token,
//                             user_home_path,
//                             NodeType::Directory,
//                             Some(permissions),
//                         );
//                         auth_reselt_to_response(Ok(()))
//                     }
//                     Err(err) => auth_reselt_to_response(Err(err)),
//                 };
//                 Ok(resp)
//             }
//             AuthOpcode::RemoveUser => {
//                 let res = self.auth.remove_user(&params.token, &params.name);
//                 let resp = match res {
//                     Ok(_) => {
//                         let user_home = format!("/home/{}", &params.name);
//                         let user_home_path = HFSPath::from(user_home.as_bytes());
//                         let _ = self.remove_file_or_directory(
//                             &params.token,
//                             user_home_path,
//                             NodeType::Directory,
//                         );
//                         auth_reselt_to_response(Ok(()))
//                     }
//                     Err(err) => auth_reselt_to_response(Err(err)),
//                 };
//                 Ok(resp)
//             }
//             AuthOpcode::CreateGroup => {
//                 let res = self.auth.create_group(&params.token, &params.name);
//                 Ok(auth_reselt_to_response(res))
//             }
//             AuthOpcode::RemoveGroup => {
//                 let res = self.auth.remove_group(&params.token, &params.name);
//                 Ok(auth_reselt_to_response(res))
//             }
//             AuthOpcode::AddUserToGroup => {
//                 let res =
//                     self.auth
//                         .add_user_to_group(&params.token, &params.name, &params.other_str);
//                 Ok(auth_reselt_to_response(res))
//             }
//             AuthOpcode::RemoveUserFromGroup => {
//                 let res = self.auth.remove_user_from_group(
//                     &params.token,
//                     &params.name,
//                     &params.other_str,
//                 );
//                 Ok(auth_reselt_to_response(res))
//             }
//         }
//     }

//     async fn open_file(
//         &self,
//         req: Request<LeaseIdTokenPath>,
//     ) -> Result<Response<BasicWithFileInfo>, volo_grpc::Status> {
//         let params = req.get_ref();
//         match self.open_file(&params.token, params.path.as_ref().into()) {
//             Ok(file_info) => {
//                 let lock_res = self
//                     .lessor
//                     .lock(
//                         params.lease_id.into(),
//                         LeaseItem::new(&NODE_LEASE_PREFIX, file_info.id),
//                     )
//                     .await;
//                 let result = match lock_res {
//                     Ok(_) => Response::new(BasicWithFileInfo {
//                         status: Status::Ok,
//                         msg: "Success!!".into(),
//                         file_info: Some(file_info),
//                     }),
//                     Err(err) => Response::new(BasicWithFileInfo {
//                         status: Status::Error,
//                         msg: err.into(),
//                         file_info: None,
//                     }),
//                 };
//                 Ok(result)
//             }
//             Err(err) => Ok(Response::new(BasicWithFileInfo {
//                 status: Status::Error,
//                 msg: err.into(),
//                 file_info: None,
//             })),
//         }
//     }

//     async fn close_file(
//         &self,
//         req: Request<LeaseFileId>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.get_ref();
//         self.lessor.unlock(
//             &params.lease_id.into(),
//             &LeaseItem::new(&NODE_LEASE_PREFIX, params.file_id),
//         );
//         Ok(Response::new(BasicResponse {
//             status: Status::Ok,
//             msg: "Closed file".into(),
//         }))
//     }
// }

// fn auth_reselt_to_response(result: Result<(), AuthError>) -> Response<BasicWithToken> {
//     match result {
//         Ok(_) => Response::new(BasicWithToken {
//             status: Status::Ok,
//             msg: "Opt success!!".into(),
//             token: None,
//         }),
//         Err(err) => Response::new(BasicWithToken {
//             status: Status::Error,
//             msg: err.into(),
//             token: None,
//         }),
//     }
// }
