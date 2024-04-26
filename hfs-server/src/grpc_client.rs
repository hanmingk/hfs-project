use std::net::SocketAddr;

use lazy_static::lazy_static;

lazy_static! {
    pub static ref CLUSTER_CLIENT: hfs_proto::cluster::ClusterClient = {
        let addr: SocketAddr = "[::1]:4090".parse().unwrap();
        hfs_proto::cluster::ClusterClientBuilder::new("cluster info service")
            .address(addr)
            .build()
    };
    pub static ref MASTER_CHUNK_CLIENT: hfs_proto::chunk::MasterChunkClient = {
        let addr: SocketAddr = "[::1]:4090".parse().unwrap();
        hfs_proto::chunk::MasterChunkClientBuilder::new("master chunk service")
            .address(addr)
            .build()
    };
}

pub fn fs_client() -> hfs_proto::master::FileSystemClient {
    let addr: SocketAddr = "[::1]:4090".parse().unwrap();
    hfs_proto::master::FileSystemClientBuilder::new("cluster info service")
        .address(addr)
        .build()
}

pub fn cluster_client() -> hfs_proto::cluster::ClusterClient {
    let addr: SocketAddr = "[::1]:4090".parse().unwrap();
    hfs_proto::cluster::ClusterClientBuilder::new("cluster info service")
        .address(addr)
        .build()
}

pub fn chunk_client(addr: SocketAddr) -> hfs_proto::chunk::ChunkClient {
    hfs_proto::chunk::ChunkClientBuilder::new("chunk service")
        .address(addr)
        .build()
}

pub fn master_chunk_client() -> hfs_proto::chunk::MasterChunkClient {
    let addr: SocketAddr = "[::1]:4090".parse().unwrap();
    hfs_proto::chunk::MasterChunkClientBuilder::new("master chunk service")
        .address(addr)
        .build()
}
