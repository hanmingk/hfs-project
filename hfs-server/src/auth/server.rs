// use std::sync::Arc;

// use hfs_proto::{
//     master::{AuthOpcode, AuthService, BasicWithToken, TokenNameOtherStrOpcode},
//     shared::Status,
// };
// use volo_grpc::{Request, Response};

// use crate::{
//     auth::Permissions,
//     namespace::{FileSystem, HFSPath, NodeType},
// };

// use super::{manager::check_login_cache_live, AuthError, AuthManager};

// pub struct UserAuthService {
//     auth_mgr: Arc<AuthManager>,
//     file_system: Arc<FileSystem>,
// }

// impl UserAuthService {
//     pub fn new(auth_mgr: Arc<AuthManager>, file_system: Arc<FileSystem>) -> UserAuthService {
//         // Start the background task.
//         tokio::spawn(check_login_cache_live(auth_mgr.clone()));

//         UserAuthService {
//             auth_mgr,
//             file_system,
//         }
//     }
// }

// impl AuthService for UserAuthService {
//     async fn auth_operations(
//         &self,
//         req: Request<TokenNameOtherStrOpcode>,
//     ) -> Result<Response<BasicWithToken>, volo_grpc::Status> {
//         let params = req.into_inner();
//         tracing::info!("auth_operations: {:?}", params);
//         match params.opcode {
//             AuthOpcode::SignIn => {
//                 let res = self.auth_mgr.sign_in(&params.name, &params.other_str);
//                 match res {
//                     Ok(token) => Ok(Response::new(BasicWithToken {
//                         status: Status::Ok,
//                         msg: "Sign in success!!".into(),
//                         token: Some(token),
//                     })),
//                     Err(err) => Ok(Response::new(BasicWithToken {
//                         status: Status::Error,
//                         msg: err.into(),
//                         token: None,
//                     })),
//                 }
//             }
//             AuthOpcode::CreateUser => {
//                 let res = self
//                     .auth_mgr
//                     .create_user(&params.token, &params.name, &params.other_str);
//                 let resp = match res {
//                     Ok(ids) => {
//                         let user_home = format!("/home/{}", &params.name);
//                         let user_home_path = HFSPath::from(user_home.as_bytes());
//                         let permissions = Permissions::with_id(ids.0, ids.1);
//                         let _ = self.file_system.create_file_or_directory(
//                             &params.token,
//                             &self.auth_mgr,
//                             user_home_path,
//                             NodeType::DirectoryNode,
//                             Some(permissions),
//                         );
//                         auth_reselt_to_response(Ok(()))
//                     }
//                     Err(err) => auth_reselt_to_response(Err(err)),
//                 };
//                 Ok(resp)
//             }
//             AuthOpcode::RemoveUser => {
//                 let res = self.auth_mgr.remove_user(&params.token, &params.name);
//                 let resp = match res {
//                     Ok(_) => {
//                         let user_home = format!("/home/{}", &params.name);
//                         let user_home_path = HFSPath::from(user_home.as_bytes());
//                         let _ = self.file_system.remove_file_or_directory(
//                             &params.token,
//                             &self.auth_mgr,
//                             user_home_path,
//                         );
//                         auth_reselt_to_response(Ok(()))
//                     }
//                     Err(err) => auth_reselt_to_response(Err(err)),
//                 };
//                 Ok(resp)
//             }
//             AuthOpcode::CreateGroup => {
//                 let res = self.auth_mgr.create_group(&params.token, &params.name);
//                 Ok(auth_reselt_to_response(res))
//             }
//             AuthOpcode::RemoveGroup => {
//                 let res = self.auth_mgr.remove_group(&params.token, &params.name);
//                 Ok(auth_reselt_to_response(res))
//             }
//             AuthOpcode::AddUserToGroup => {
//                 let res =
//                     self.auth_mgr
//                         .add_user_to_group(&params.token, &params.name, &params.other_str);
//                 Ok(auth_reselt_to_response(res))
//             }
//             AuthOpcode::RemoveUserFromGroup => {
//                 let res = self.auth_mgr.remove_user_from_group(
//                     &params.token,
//                     &params.name,
//                     &params.other_str,
//                 );
//                 Ok(auth_reselt_to_response(res))
//             }
//         }
//     }
// }

// fn auth_reselt_to_response(result: Result<(), AuthError>) -> Response<BasicWithToken> {
//     match result {
//         Ok(_) => Response::new(BasicWithToken {
//             status: Status::Ok,
//             msg: "OP success!!".into(),
//             token: None,
//         }),
//         Err(err) => Response::new(BasicWithToken {
//             status: Status::Error,
//             msg: err.into(),
//             token: None,
//         }),
//     }
// }
