// use std::{result::Result, sync::Arc};

// use hfs_proto::{
//     dlock::{BasicWithLeaseId, DLockService, LeaseId, LeaseIdLeaseItem, LeaseIdTime},
//     shared::{BasicResponse, EmptyParams, Status},
// };
// use tokio::{sync::mpsc, time::Duration};
// use volo_grpc::{Request, Response};

// use crate::{shutdown::Shutdown, util::grpc::result_to_response};

// use super::Lessor;

// #[derive(Debug)]
// pub struct DLockServer {
//     lessor: Arc<Lessor>,
// }

// impl DLockServer {
//     pub fn new(
//         lessor: Arc<Lessor>,
//         shutdown: Shutdown,
//         shutdown_complete: mpsc::Sender<()>,
//     ) -> DLockServer {
//         // Start the background task.
//         tokio::spawn(check_lease_alive_tasks(
//             lessor.clone(),
//             shutdown,
//             shutdown_complete,
//         ));

//         DLockServer { lessor }
//     }
// }

// async fn check_lease_alive_tasks(
//     lessor: Arc<Lessor>,
//     mut shutdown: Shutdown,
//     _shutdown_complete: mpsc::Sender<()>,
// ) {
//     static INTERVAL_TIME: Duration = tokio::time::Duration::from_secs(1);
//     while !shutdown.is_shutdown() {
//         let when = tokio::time::Instant::now() + INTERVAL_TIME;

//         // If the lease does not exist, the process is blocked and waits for the next execution.
//         tokio::select! {
//             _ = lessor.check_lease_alive() => {},
//             _ = shutdown.recv() => break,
//         }

//         tokio::select! {
//             _ = tokio::time::sleep_until(when) => {}
//             _ = shutdown.recv() => break,
//         }
//     }

//     tracing::info!("Need to shutdown!");
//     lessor.do_shutdown();
//     tracing::info!("Shutdown complete");
// }

// impl DLockService for DLockServer {
//     async fn grant_lease(
//         &self,
//         _req: Request<EmptyParams>,
//     ) -> Result<Response<BasicWithLeaseId>, volo_grpc::Status> {
//         let lease_id = self.lessor.apply_lease();

//         Ok(Response::new(BasicWithLeaseId {
//             status: Status::Ok,
//             msg: "Success!!".into(),
//             lease_id: Some(lease_id),
//         }))
//     }

//     async fn keep_alive(
//         &self,
//         req: Request<LeaseIdTime>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.get_ref();
//         let res = self
//             .lessor
//             .update_lease(&params.lease_id, Duration::new(params.time, 0));
//         Ok(Response::new(BasicResponse {
//             status: match res {
//                 true => Status::Ok,
//                 false => Status::Error,
//             },
//             msg: match res {
//                 true => "Success!!".into(),
//                 false => "Unknow lease".into(),
//             },
//         }))
//     }

//     async fn lock(
//         &self,
//         req: Request<LeaseIdLeaseItem>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.get_ref();
//         let res = self
//             .lessor
//             .lock(params.lease_id, params.lease_item.to_vec().into())
//             .await;

//         Ok(result_to_response(res))
//     }

//     async fn unlock(
//         &self,
//         req: Request<LeaseIdLeaseItem>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.get_ref();
//         self.lessor
//             .unlock(&params.lease_id, &params.lease_item.to_vec().into());

//         Ok(Response::new(BasicResponse {
//             status: Status::Ok,
//             msg: "Success!!".into(),
//         }))
//     }

//     async fn try_lock(
//         &self,
//         _req: Request<LeaseIdLeaseItem>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         // TODO
//         Ok(Response::new(BasicResponse {
//             status: Status::Ok,
//             msg: "Success!!".into(),
//         }))
//     }

//     async fn cancel_lease(
//         &self,
//         req: Request<LeaseId>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let params = req.get_ref();
//         self.lessor.cancel_lease(&params.lease_id);
//         Ok(Response::new(BasicResponse {
//             status: Status::Ok,
//             msg: "Success!!".into(),
//         }))
//     }
// }
