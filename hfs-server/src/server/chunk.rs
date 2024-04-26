use std::{
    future::Future,
    io::{self, SeekFrom},
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};

use ahash::AHashMap;
use hfs_proto::{
    chunk::ChunkServer,
    cluster::{member_add_request::ServicePort, HeartbeatRequest, MemberAddRequest, ServiceType},
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::TcpListener,
    sync::{broadcast, mpsc},
};
use volo::FastStr;
use volo_grpc::server::{Server, ServiceBuilder};

use crate::{
    chunk::chunkserver::{
        manager::{start_report, ReplicaManager},
        replica::{history_from, ReplicaMetaInfo},
        ReplicaService,
    },
    grpc_client::CLUSTER_CLIENT,
    shutdown::Shutdown,
    transport::{chunkserver::service::TransportService, replica_file::VERSION_POS},
    util::gen_id::GenIdI64,
};

use super::cfg::HFS_HOME;

pub async fn run(
    tcp_addr: SocketAddr,
    grpc_addr: SocketAddr,
    server_name: &str,
    shutdown: impl Future,
) {
    let Ok(mut replica_manager) = replica_manager(server_name).await else {
        tracing::error!("Loading replica meta information error");
        return;
    };

    // Joining cluster
    match CLUSTER_CLIENT
        .member_add(MemberAddRequest {
            server_id: replica_manager.server_id,
            addr: FastStr::from("127.0.0.1"),
            service_ports: vec![
                ServicePort {
                    service_type: ServiceType::Rpc,
                    port: grpc_addr.port() as u32,
                },
                ServicePort {
                    service_type: ServiceType::Tcp,
                    port: tcp_addr.port() as u32,
                },
            ],
        })
        .await
    {
        Ok(resp) => {
            replica_manager.server_id = resp.into_inner().server_id;
            tracing::info!(
                server_id = replica_manager.server_id,
                "{} server joining cluster success",
                server_name,
            );
        }
        Err(err) => {
            tracing::error!(status = %err, "{} server joining cluster error", server_name);
            return;
        }
    }

    // Heartbeat task
    tokio::spawn(report_heartbeat(replica_manager.server_id));

    let (notify_shutdown, _) = broadcast::channel::<()>(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);

    let replica_manager = Arc::new(replica_manager);

    let replic_service = ReplicaService::new(replica_manager.clone());

    let addr = volo::net::Address::from(grpc_addr);
    let grpc_service = Server::new()
        .add_service(ServiceBuilder::new(ChunkServer::new(replic_service)).build())
        .run(addr);

    match TcpListener::bind(tcp_addr).await {
        Ok(listener) => {
            let mut transport_service = TransportService {
                replica_mgr: replica_manager.clone(),
                listener,
                shutdown_complete: shutdown_complete_tx,
            };
            let mut shutdown = Shutdown::new(notify_shutdown.subscribe());

            tracing::info!("Transport service listens on: {:?}", tcp_addr);

            tokio::spawn(async move {
                tokio::select! {
                    res = transport_service.run() => {
                        if let Err(err) = res {
                            tracing::error!(error = %err, "Transport service accept error");
                        }
                    },
                    _ = shutdown.recv() => {}
                };
            });
        }
        Err(err) => {
            tracing::error!(error = %err, "Transport tcpListener bind error");
            return;
        }
    }

    start_report(&replica_manager).await;

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

    let _ = shutdown_complete_rx.recv().await;

    if let Err(err) = do_shutdown(replica_manager).await {
        tracing::error!(error = %err, "{} server shutdown task error", server_name);
    }
    tracing::info!("{} server has shutdown!!", server_name)
}

async fn init(server_name: &str) -> io::Result<(u32, i64)> {
    let server_home = format!("{}/{}", HFS_HOME, server_name);
    let server_path = Path::new(&server_home);

    if !server_path.exists() {
        fs::create_dir_all(server_path).await?;
    }

    let data_home = format!("{}/data", server_home);
    let data_path = Path::new(&data_home);

    if !data_path.exists() {
        fs::create_dir(data_path).await?;
    }

    let image_src = format!("{}/replica_image", server_home);
    let path = Path::new(&image_src);

    let Ok(mut image_file) = File::open(path).await else {
        return Ok((0, 0));
    };

    Ok((image_file.read_u32().await?, image_file.read_i64().await?))
}

async fn replica_manager(server_name: &str) -> io::Result<ReplicaManager> {
    let (server_id, atomic_id) = init(server_name).await?;

    let cd_path = format!("{}/{}/data", HFS_HOME, server_name);
    let path = Path::new(&cd_path);

    if !path.exists() {
        fs::create_dir_all(path).await?;
    }

    let mut meta_map = AHashMap::new();
    get_meta(&mut meta_map, path).await?;

    Ok(ReplicaManager {
        replica_meta: Mutex::new(meta_map),
        update_map: Mutex::new(AHashMap::new()),
        server_id,
        gen_id: GenIdI64::new(atomic_id),
        base_dir: format!("{}/{}", HFS_HOME, server_name),
    })
}

async fn get_meta(
    meta_map: &mut AHashMap<i64, ReplicaMetaInfo>,
    root_dir: &Path,
) -> io::Result<()> {
    let mut stack = Vec::new();
    stack.push(root_dir.to_path_buf());

    while let Some(dir) = stack.pop() {
        let Ok(mut entries) = fs::read_dir(dir).await else {
            continue;
        };

        while let Some(entry) = entries.next_entry().await? {
            let child_path = entry.path();

            if child_path.is_dir() {
                stack.push(child_path);
            } else if let Some(file_name) = child_path.file_name() {
                let Some(file_name_str) = file_name.to_str() else {
                    continue;
                };

                if !file_name_str.ends_with(".meta") || !file_name_str.starts_with("chunk_") {
                    continue;
                }

                if let Some(meta_info) = from_file(&child_path).await {
                    meta_map.insert(meta_info.replica_id, meta_info);
                }
            }
        }
    }

    Ok(())
}

async fn from_file(path: &Path) -> Option<ReplicaMetaInfo> {
    let mut meta_file = File::open(path).await.ok()?;
    meta_file.seek(SeekFrom::Start(VERSION_POS)).await.ok()?;

    let history_info = history_from(&mut meta_file).await.ok()?;

    history_info.last().cloned()
}

async fn report_heartbeat(server_id: u32) {
    const INTERVALS: std::time::Duration = std::time::Duration::from_secs(20);

    loop {
        if let Err(err) = CLUSTER_CLIENT
            .heartbeat(HeartbeatRequest {
                server_id,
                cpu_load: 0.0,
                disk_load: 0.0,
                network_load: 0.0,
            })
            .await
        {
            tracing::error!(cause = %err, "Chunk server report heartbeat error");
            return;
        }

        let next_time = tokio::time::Instant::now() + INTERVALS;
        tokio::time::sleep_until(next_time).await;
    }
}

async fn do_shutdown(replica_manager: Arc<ReplicaManager>) -> io::Result<()> {
    let mut replica_image = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&format!("{}/replica_image", replica_manager.base_dir))
        .await?;

    replica_image.write_u32(replica_manager.server_id).await?;
    replica_image
        .write_i64(replica_manager.next_replica_id())
        .await?;

    Ok(())
}
