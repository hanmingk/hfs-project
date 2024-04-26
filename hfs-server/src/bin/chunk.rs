use std::{env, net::SocketAddr};

use hfs_server::server::chunk;
use tokio::signal;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        tracing::error!("Unknown args!!");
        return;
    }

    let port: u16 = args[2].parse().unwrap();

    let tcp_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let grpc_addr: SocketAddr = format!("127.0.0.1:{}", port + 1).parse().unwrap();

    chunk::run(tcp_addr, grpc_addr, &args[1], signal::ctrl_c()).await;
}
