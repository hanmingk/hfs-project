mod error;

pub mod server;
pub mod service;
pub mod watcher;

pub mod cfg {
    use std::time::Duration;

    pub const MAX_LIVE_TIME: Duration = Duration::from_secs(25);
}

pub use service::ClusterService;
pub use watcher::ClusterWatcher;
