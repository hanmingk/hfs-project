use faststr::FastStr;
use hfs_proto::master::{ContainsPathRequest, FileSystemClient};

pub async fn hfs_cd(fs_client: &FileSystemClient, path: FastStr) -> bool {
    if let Ok(resp) = fs_client
        .contains_path(ContainsPathRequest { path: path })
        .await
    {
        return resp.get_ref().is_dir;
    }

    false
}
