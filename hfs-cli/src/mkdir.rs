use faststr::FastStr;
use hfs_proto::master::{CreateDirectoryRequest, FileSystemClient};

pub async fn hfs_mkdir(fs_client: &FileSystemClient, token: FastStr, path: FastStr, parents: bool) {
    if let Err(err) = fs_client
        .create_directory(CreateDirectoryRequest {
            token,
            dir_path: path,
            parent: parents,
        })
        .await
    {
        println!("{}", err.message());
    }
}
