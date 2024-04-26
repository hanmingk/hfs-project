use std::net::SocketAddr;

use hfs_server::server::master;
use tokio::signal;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = "[::]:4090".parse().unwrap();

    master::run(addr, signal::ctrl_c()).await
}
