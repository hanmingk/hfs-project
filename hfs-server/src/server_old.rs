// use std::{future::Future, net::SocketAddr, sync::Arc};

// use hfs_proto::{
//     cluster::ClusterServiceServer,
//     dlock::DLockServiceServer,
//     master::{AuthServiceServer, FileSystemServiceServer},
// };
// use tokio::sync::{broadcast, mpsc};
// use volo_grpc::server::{Server, ServiceBuilder};

// use crate::{
//     auth::{AuthManager, UserAuthService},
//     chunk::ChunkManager,
//     cluster::{ClusterGuard, ClusterServer},
//     lease::{DLockServer, Lessor},
//     namespace::{FileNameSpace, FileSystem},
//     shutdown::Shutdown,
// };

// pub async fn run(socket_addr: SocketAddr, shutdown: impl Future) {
//     let (notify_shutdown, _) = broadcast::channel(1);
//     let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

//     let cluster_guard = Arc::new(ClusterGuard::new());
//     let lessor = Arc::new(Lessor::new());
//     let chunk_manager = Arc::new(ChunkManager::new(6));
//     let auth_mgr = Arc::new(AuthManager::new());
//     let file_system = Arc::new(FileSystem::new());

//     let cluster_service = ClusterServer::new(cluster_guard.clone());

//     let lease_service = DLockServer::new(
//         lessor.clone(),
//         Shutdown::new(notify_shutdown.subscribe()),
//         shutdown_complete_tx.clone(),
//     );

//     let auth_service = UserAuthService::new(auth_mgr.clone(), file_system.clone());

//     let file_service = FileNameSpace {
//         file_system,
//         chunk_manager,
//         cluster: cluster_guard,
//         auth: auth_mgr,
//         lessor,
//     };

//     let addr = volo::net::Address::from(socket_addr);
//     let grpc_service = Server::new()
//         .add_service(ServiceBuilder::new(ClusterServiceServer::new(cluster_service)).build())
//         .add_service(ServiceBuilder::new(DLockServiceServer::new(lease_service)).build())
//         .add_service(ServiceBuilder::new(AuthServiceServer::new(auth_service)).build())
//         .add_service(ServiceBuilder::new(FileSystemServiceServer::new(file_service)).build())
//         .run(addr);

//     tokio::select! {
//         res = grpc_service => {
//             if let Err(err) = res {
//                 tracing::error!(cause = %err, "failed to accept");
//             }
//         }
//         _ = shutdown => {
//             // The shutdown signal has been received.
//             tracing::info!("shutting down");
//         }
//     }

//     drop(notify_shutdown);

//     drop(shutdown_complete_tx);

//     let _ = shutdown_complete_rx.recv().await;
//     tracing::info!("shutting down task over!!");
// }
