use hfs_proto::cluster::ChunkServerInfo;

#[derive(Debug)]
pub struct ChunkServer {
    id: u32,
    address: [u8; 4],
    status: ServerStatus,
    cpu_load: f32,
    disk_load: f32,
}

#[derive(Debug)]
enum ServerStatus {
    Online,
    Offline,
    Maintenance,
}

impl ChunkServer {
    pub fn with_info(chunk_info: ChunkServerInfo) -> Self {
        let address = [
            chunk_info.address[0],
            chunk_info.address[1],
            chunk_info.address[2],
            chunk_info.address[3],
        ];
        ChunkServer {
            id: chunk_info.server_id,
            address,
            status: ServerStatus::Online,
            cpu_load: chunk_info.cpu_load,
            disk_load: chunk_info.disk_load,
        }
    }

    pub fn update_with_info(&mut self, info: ChunkServerInfo) {
        self.cpu_load = info.cpu_load;
        self.disk_load = info.disk_load;
    }

    pub fn addr(&self) -> &[u8] {
        &self.address
    }
}
