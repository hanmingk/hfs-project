// use std::{result::Result, sync::Arc};

// use hfs_proto::{
//     cluster::{BasicWithServerId, ClusterService, ReportInfo, ServerAddr},
//     shared::{BasicResponse, Status},
// };
// use volo_grpc::{Request, Response};

// use crate::util::grpc::result_to_response;

// use super::{guard::check_heartbeat_task, ClusterGuard};

// #[derive(Debug)]
// pub struct ClusterServer {
//     guard: Arc<ClusterGuard>,
// }

// impl ClusterServer {
//     pub fn new(guard: Arc<ClusterGuard>) -> ClusterServer {
//         tokio::spawn(check_heartbeat_task(guard.clone()));
//         ClusterServer { guard }
//     }
// }

// impl ClusterService for ClusterServer {
//     async fn report_heartbeat(
//         &self,
//         req: Request<ReportInfo>,
//     ) -> Result<Response<BasicResponse>, volo_grpc::Status> {
//         let server_info = req.get_ref();
//         let result = self.guard.report_heartbeat(server_info.clone());
//         Ok(result_to_response(result))
//     }

//     async fn join_cluster(
//         &self,
//         req: Request<ServerAddr>,
//     ) -> Result<Response<BasicWithServerId>, volo_grpc::Status> {
//         let server_addr = req.into_inner();
//         let server_id = self.guard.join_cluster(server_addr.addr);

//         Ok(Response::new(BasicWithServerId {
//             status: Status::Ok,
//             msg: "Success!!".into(),
//             server_id: Some(server_id),
//         }))
//     }
// }
