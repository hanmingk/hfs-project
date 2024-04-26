use ahash::AHashMap;
use hfs_proto::cluster::ServiceType;

#[derive(Debug)]
pub struct ChunkServer {
    pub server_id: u32,
    pub addr: [u8; 4],
    pub service_ports: AHashMap<ServiceType, u16>,
    pub status: ServerStatus,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ServerStatus {
    Online,
    Offline,
}
