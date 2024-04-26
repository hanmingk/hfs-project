use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};

use faststr::FastStr;
use hfs_proto::master::{ContainsPathRequest, FileSystemClient};
use hfs_server::transport::client::input::HFSInputStream;
use tokio::{
    fs::{self, File},
    io::{AsyncSeekExt, AsyncWriteExt},
};

pub async fn hfs_get(
    fs_client: &FileSystemClient,
    token: FastStr,
    mut local_path: PathBuf,
    hfs_path: FastStr,
) -> Result<(), String> {
    match fs_client
        .contains_path(ContainsPathRequest {
            path: hfs_path.clone(),
        })
        .await
    {
        Ok(resp) if !resp.get_ref().is_dir => {}
        _ => return Err("No such file or directory".into()),
    }

    if local_path.is_dir() {
        let file_name = Path::new(hfs_path.as_str()).file_name().unwrap();
        local_path.push(file_name);
    } else {
        if local_path.is_file() {
            return Err("Local file exist".into());
        }
    }

    let Ok(mut local_file) = File::create(&local_path).await else {
        return Err("Local file create error".into());
    };

    match HFSInputStream::open(token, hfs_path).await {
        Ok(mut input_stream) => {
            if let Err(err) = read_data(&mut input_stream, &mut local_file).await {
                drop(local_file);

                let _ = fs::remove_file(local_path).await;

                let _ = input_stream.close().await;

                return Err(err);
            }

            let _ = input_stream.close().await;
            Ok(())
        }
        Err(err) => {
            drop(local_file);

            let _ = fs::remove_file(local_path).await;

            Err(err.to_string())
        }
    }
}

async fn read_data(input_stream: &mut HFSInputStream, local_file: &mut File) -> Result<(), String> {
    let mut buf = [0; 1024 * 4];

    loop {
        match input_stream.read(&mut buf).await {
            Ok(n) if n < 0 => {
                if let Err(err) = local_file.seek(SeekFrom::Current(n)).await {
                    return Err(err.to_string());
                }
            }
            Ok(n) if n == 0 => break,
            Ok(n) => {
                if let Err(err) = local_file.write_all(&buf[..n as usize]).await {
                    return Err(err.to_string());
                }
            }
            Err(err) => {
                return Err(err.to_string());
            }
        }
    }

    Ok(())
}
