use std::sync::Arc;

use hfs_proto::master::{
    Auth, CreateGroupRequest, CreateGroupResponse, CreateUserRequest, CreateUserResponse,
    GroupAddUserRequest, GroupAddUserResponse, GroupRmUserRequest, GroupRmUserResponse,
    KeepHeartbeatRequest, KeepHeartbeatResponse, RemoveGroupRequest, RemoveGroupResponse,
    RemoveUserRequest, RemoveUserResponse, SignInRequest, SignInResponse,
};
use volo_grpc::{Request, Response, Status};

use crate::{
    auth::Permissions,
    namespace::{FileSystem, HFSPath, NodeType},
    shutdown::Shutdown,
};

use super::{manager::check_login_cache_live, AuthManager};

#[derive(Debug)]
pub struct AuthService {
    auth_mgr: Arc<AuthManager>,
    file_system: Arc<FileSystem>,
}

impl AuthService {
    pub fn new(auth: Arc<AuthManager>, file_system: Arc<FileSystem>) -> Self {
        AuthService {
            auth_mgr: auth,
            file_system,
        }
    }

    pub fn backgroud_task(&self, shutdown: Shutdown) {
        tokio::spawn(check_login_cache_live(self.auth_mgr.clone(), shutdown));
    }
}

impl Auth for AuthService {
    async fn sign_in(
        &self,
        req: Request<SignInRequest>,
    ) -> Result<Response<SignInResponse>, Status> {
        let req_param = req.into_inner();
        let token = self
            .auth_mgr
            .sign_in(&req_param.username, &req_param.passwd)?;

        tracing::info!("{} has sign!!", req_param.username);

        Ok(Response::new(SignInResponse { token }))
    }

    async fn keep_heartbeat(
        &self,
        req: Request<KeepHeartbeatRequest>,
    ) -> Result<Response<KeepHeartbeatResponse>, Status> {
        let req_param = req.into_inner();

        self.auth_mgr.keep_heartbeat(&req_param.token)?;

        Ok(Response::new(KeepHeartbeatResponse {}))
    }

    async fn create_user(
        &self,
        req: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        let req_param = req.into_inner();
        let ids =
            self.auth_mgr
                .create_user(&req_param.token, &req_param.username, &req_param.passwd)?;

        let user_home = format!("/home/{}", &req_param.username);

        let permissions = Permissions::with_id(ids.0, ids.1);
        self.file_system.create_file_or_directory(
            &req_param.token,
            &self.auth_mgr,
            user_home.as_bytes().into(),
            NodeType::DirectoryNode,
            Some(permissions),
        )?;

        Ok(Response::new(CreateUserResponse {}))
    }

    async fn remove_user(
        &self,
        req: Request<RemoveUserRequest>,
    ) -> Result<Response<RemoveUserResponse>, Status> {
        let req_param = req.into_inner();
        self.auth_mgr
            .remove_user(&req_param.token, &req_param.username)?;
        let user_home = format!("/home/{}", &req_param.username);
        let user_home_path = HFSPath::from(user_home.as_bytes());
        self.file_system.remove_file_or_directory(
            &req_param.token,
            &self.auth_mgr,
            user_home_path,
        )?;

        Ok(Response::new(RemoveUserResponse {}))
    }

    async fn create_group(
        &self,
        req: Request<CreateGroupRequest>,
    ) -> Result<Response<CreateGroupResponse>, Status> {
        let req_param = req.into_inner();
        self.auth_mgr
            .create_group(&req_param.token, &req_param.group_name)?;

        Ok(Response::new(CreateGroupResponse {}))
    }

    async fn remove_group(
        &self,
        req: Request<RemoveGroupRequest>,
    ) -> Result<Response<RemoveGroupResponse>, Status> {
        let req_param = req.into_inner();
        self.auth_mgr
            .remove_group(&req_param.token, &req_param.group_name)?;

        Ok(Response::new(RemoveGroupResponse {}))
    }

    async fn group_add_user(
        &self,
        req: Request<GroupAddUserRequest>,
    ) -> Result<Response<GroupAddUserResponse>, Status> {
        let req_param = req.into_inner();

        self.auth_mgr.add_user_to_group(
            &req_param.token,
            &req_param.group_name,
            &req_param.username,
        )?;

        Ok(Response::new(GroupAddUserResponse {}))
    }

    async fn group_rm_user(
        &self,
        req: Request<GroupRmUserRequest>,
    ) -> Result<Response<GroupRmUserResponse>, Status> {
        let req_param = req.into_inner();

        self.auth_mgr.remove_user_from_group(
            &req_param.token,
            &req_param.group_name,
            &req_param.username,
        )?;

        Ok(Response::new(GroupRmUserResponse {}))
    }
}
