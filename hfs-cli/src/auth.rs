use faststr::FastStr;
use hfs_proto::master::{
    AuthClient, CreateGroupRequest, CreateUserRequest, GroupAddUserRequest, GroupRmUserRequest,
    RemoveGroupRequest, RemoveUserRequest,
};

pub async fn user_add(
    auth_client: &AuthClient,
    token: FastStr,
    username: FastStr,
    passwd: FastStr,
) {
    if let Err(err) = auth_client
        .create_user(CreateUserRequest {
            token,
            username,
            passwd,
        })
        .await
    {
        println!("{}", err.message());
    }
}

pub async fn user_remove(auth_client: &AuthClient, token: FastStr, username: FastStr) {
    if let Err(err) = auth_client
        .remove_user(RemoveUserRequest { token, username })
        .await
    {
        println!("{}", err.message());
    }
}

pub async fn group_add(auth_client: &AuthClient, token: FastStr, group_name: FastStr) {
    if let Err(err) = auth_client
        .create_group(CreateGroupRequest { token, group_name })
        .await
    {
        println!("{}", err.message());
    }
}

pub async fn group_remove(auth_client: &AuthClient, token: FastStr, group_name: FastStr) {
    if let Err(err) = auth_client
        .remove_group(RemoveGroupRequest { token, group_name })
        .await
    {
        println!("{}", err.message());
    }
}

pub async fn group_add_user(
    auth_client: &AuthClient,
    token: FastStr,
    group_name: FastStr,
    username: FastStr,
) {
    if let Err(err) = auth_client
        .group_add_user(GroupAddUserRequest {
            token,
            group_name,
            username,
        })
        .await
    {
        println!("{}", err.message());
    }
}

pub async fn group_remove_user(
    auth_client: &AuthClient,
    token: FastStr,
    group_name: FastStr,
    username: FastStr,
) {
    if let Err(err) = auth_client
        .group_rm_user(GroupRmUserRequest {
            token,
            group_name,
            username,
        })
        .await
    {
        println!("{}", err.message());
    }
}
