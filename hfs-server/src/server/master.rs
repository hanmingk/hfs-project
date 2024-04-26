use std::{future::Future, net::SocketAddr, path::Path, sync::Arc};

use hfs_proto::{
    chunk::MasterChunkServer,
    cluster::ClusterServer,
    lease::LeaseServer,
    master::{AuthServer, FileSystemServer},
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast, mpsc},
};
use volo_grpc::server::{Server, ServiceBuilder};

use crate::{
    auth::{auth_deserialize, AuthManager, AuthService},
    chunk::masterserver::{ChunkManager, ChunkMetaService},
    cluster::{ClusterService, ClusterWatcher},
    lease::{LeaseService, Lessor},
    namespace::{fd_deserialize, FileSystem, FileSystemService},
    shutdown::Shutdown,
};

use super::cfg::{BUCKET_SIZE, MASTER_HOME};

pub async fn run(socket_addr: SocketAddr, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel::<()>(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);

    let cluster_watcher = Arc::new(ClusterWatcher::new());

    let lessor = Arc::new(Lessor::new());

    let chunk_manager = match get_atomic().await {
        Ok(atomic) => Arc::new(ChunkManager::new(BUCKET_SIZE, atomic)),
        Err(err) => {
            tracing::error!(error = %err, "ChunkManager init error");
            return;
        }
    };

    let auth = match get_auth().await {
        Ok(res) => Arc::new(res),
        Err(err) => {
            tracing::error!(error = %err, "AuthManager init error");
            return;
        }
    };

    let file_system = match get_filesystem().await {
        Ok(res) => Arc::new(res),
        Err(err) => {
            tracing::error!(error = %err, "FileSystem init error");
            return;
        }
    };

    let cluster_service = ClusterService::new(cluster_watcher.clone());
    cluster_service.backgroud_task(Shutdown::new(notify_shutdown.subscribe()));

    let lease_service = LeaseService::new(lessor.clone());
    lease_service.backgroud_task(
        Shutdown::new(notify_shutdown.subscribe()),
        shutdown_complete_tx.clone(),
    );

    let auth_service = AuthService::new(auth.clone(), file_system.clone());
    auth_service.backgroud_task(Shutdown::new(notify_shutdown.subscribe()));

    let chunk_service = ChunkMetaService::new(chunk_manager.clone(), cluster_watcher.clone());
    chunk_service.backgroud_task(Shutdown::new(notify_shutdown.subscribe()));

    let file_service = FileSystemService {
        file_system: file_system.clone(),
        chunk_manager: chunk_manager.clone(),
        cluster: cluster_watcher,
        auth: auth.clone(),
        lessor,
    };

    let addr = volo::net::Address::from(socket_addr);
    let grpc_service = Server::new()
        .add_service(ServiceBuilder::new(ClusterServer::new(cluster_service)).build())
        .add_service(ServiceBuilder::new(LeaseServer::new(lease_service)).build())
        .add_service(ServiceBuilder::new(AuthServer::new(auth_service)).build())
        .add_service(ServiceBuilder::new(MasterChunkServer::new(chunk_service)).build())
        .add_service(ServiceBuilder::new(FileSystemServer::new(file_service)).build())
        .run(addr);

    tokio::select! {
        res = grpc_service => {
            if let Err(err) = res {
                tracing::error!(error = %err, "grpc service error");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            tracing::info!("shutting down");
        }
    }

    drop(notify_shutdown);

    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;

    if let Err(err) = do_shutdown(&file_system, &auth, chunk_manager.next_id()).await {
        tracing::error!(error = %err, "Master server shudown task error");
    }

    tracing::info!("HFS master server has shutdown!!");
}

async fn get_filesystem() -> crate::HFSResult<FileSystem> {
    let path = format!("{}/fsimage", MASTER_HOME);
    let fsimage_path = Path::new(&path);

    if !fsimage_path.exists() {
        return Ok(FileSystem::new());
    }

    let mut reader = File::open(fsimage_path).await?;
    let atomic_id = reader.read_i64().await?;

    let fs_directory = fd_deserialize(&mut reader).await?;

    Ok(FileSystem::from(fs_directory, atomic_id))
}

async fn get_atomic() -> crate::HFSResult<i64> {
    let path = format!("{}/chunk_image", MASTER_HOME);
    let chunk_path = Path::new(&path);

    if !chunk_path.exists() {
        return Ok(0);
    }

    let mut reader = File::open(chunk_path).await?;

    Ok(reader.read_i64().await?)
}

async fn get_auth() -> crate::HFSResult<AuthManager> {
    let path = format!("{}/auth_image", MASTER_HOME);
    let auth_path = Path::new(&path);

    if !auth_path.exists() {
        return Ok(AuthManager::new());
    }

    let mut reader = File::open(auth_path).await?;

    Ok(auth_deserialize(&mut reader).await?)
}

async fn do_shutdown(fs: &FileSystem, auth: &AuthManager, atomic: i64) -> crate::HFSResult<()> {
    let mut fs_image = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&format!("{}/fsimage", MASTER_HOME))
        .await?;

    fs.serilize(&mut fs_image).await?;

    let mut auth_image = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&format!("{}/auth_image", MASTER_HOME))
        .await?;

    auth.serilize(&mut auth_image).await?;

    let mut chunk_image = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&format!("{}/chunk_image", MASTER_HOME))
        .await?;

    chunk_image.write_i64(atomic).await?;

    Ok(())
}
