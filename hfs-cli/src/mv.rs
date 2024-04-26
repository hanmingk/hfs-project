use faststr::FastStr;
use hfs_proto::master::{FileSystemClient, MovePathRequest};

pub async fn hfs_mv(
    fs_client: &FileSystemClient,
    token: FastStr,
    src_path: FastStr,
    target_path: FastStr,
) {
    if let Err(err) = fs_client
        .move_path(MovePathRequest {
            token,
            src_path,
            target_path,
        })
        .await
    {
        println!("{}", err.message());
    }
}
