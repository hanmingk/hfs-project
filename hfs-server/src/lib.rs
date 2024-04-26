pub mod auth;
pub mod chunk;
pub mod cluster;
pub mod grpc_client;
pub mod lease;
pub mod namespace;
pub mod server;
pub mod shutdown;
pub mod transport;
pub mod util;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type HFSResult<T> = std::result::Result<T, Error>;
