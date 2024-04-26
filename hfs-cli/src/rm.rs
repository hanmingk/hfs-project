use faststr::FastStr;
use hfs_proto::master::{FileSystemClient, RemoveFileRequest};

pub async fn hfs_rm(
    fs_client: &FileSystemClient,
    token: FastStr,
    path: FastStr,
    _recursive: bool,
) -> Result<(), String> {
    if let Err(err) = fs_client
        .remove_file(RemoveFileRequest {
            token,
            file_path: path,
        })
        .await
    {
        return Err(err.message().into());
    }

    Ok(())
}
