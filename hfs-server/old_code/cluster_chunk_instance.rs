// use std::time::Instant;

// use hfs_proto::cluster::ReportInfo;

// #[derive(Debug)]
// pub struct ChunkInstance {
//     server_id: u32,
//     addr: String,
//     status: ServerStatus,
//     cpu_load: f32,
//     disk_load: f32,
//     network_load: f32,
//     report_time: Instant,
// }

// #[derive(Debug)]
// enum ServerStatus {
//     Online,
//     Offline,
// }

// impl ChunkInstance {
//     pub fn new(id: u32, addr: String) -> ChunkInstance {
//         ChunkInstance {
//             server_id: id,
//             addr,
//             status: ServerStatus::Online,
//             cpu_load: 0.0,
//             disk_load: 0.0,
//             network_load: 0.0,
//             report_time: Instant::now(),
//         }
//     }

//     pub fn update_with_info(&mut self, report_info: ReportInfo) {
//         self.cpu_load = report_info.cpu_load;
//         self.disk_load = report_info.disk_load;
//         self.network_load = report_info.network_load;
//         self.report_time = Instant::now();
//     }

//     pub fn is_live(&self, ct: &Instant) -> bool {
//         static MAX_LIVE_TIME: std::time::Duration = std::time::Duration::from_secs(25);
//         if ct.duration_since(self.report_time) < MAX_LIVE_TIME {
//             return true;
//         }
//         false
//     }

//     pub fn addr(&self) -> &str {
//         &self.addr
//     }
// }

// pub struct ReportInfo {
//     pub server_id: u32,
//     pub cpu_load: f32,
//     pub disk_load: f32,
//     pub network_load: f32,
// }
