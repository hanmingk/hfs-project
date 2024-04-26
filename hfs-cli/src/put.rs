use std::path::PathBuf;

use faststr::FastStr;
use hfs_proto::master::{ContainsPathRequest, CreateFileRequest, FileSystemClient};
use hfs_server::transport::client::output::HFSOutputStream;
use tokio::fs::File;

pub async fn hfs_put(
    fs_client: &FileSystemClient,
    token: FastStr,
    local_path: PathBuf,
    hfs_path: FastStr,
) -> Result<(), String> {
    if !local_path.exists() || local_path.is_dir() {
        return Err("No such file or directory".into());
    }

    let file_path = match fs_client
        .contains_path(ContainsPathRequest {
            path: hfs_path.clone(),
        })
        .await
    {
        Ok(resp) => {
            if resp.into_inner().is_dir {
                let mut path1 = hfs_path.to_string();
                path1.push('/');
                path1.push_str(local_path.file_name().unwrap().to_str().unwrap());

                let path1 = FastStr::from_string(path1);

                hfs_touch(fs_client, token.clone(), path1.clone()).await?;

                path1
            } else {
                hfs_path
            }
        }
        Err(_) => {
            hfs_touch(fs_client, token.clone(), hfs_path.clone()).await?;

            hfs_path
        }
    };

    let mut output_stream = HFSOutputStream::open(token.clone(), file_path).await?;

    let Ok(mut data_file) = File::open(local_path).await else {
        let _ = output_stream.close().await;
        return Err("No sucn file or directory".into());
    };

    if let Err(err) = output_stream.output_stream(&mut data_file).await {
        let _ = output_stream.close().await;
        return Err(err.to_string());
    }

    output_stream.close().await
}

pub async fn hfs_touch(
    fs_client: &FileSystemClient,
    token: FastStr,
    path: FastStr,
) -> Result<(), String> {
    if let Err(err) = fs_client
        .create_file(CreateFileRequest {
            token,
            file_path: path,
        })
        .await
    {
        return Err(err.message().into());
    }

    Ok(())
}
