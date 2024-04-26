mod error;
mod lease;
mod lessor;

pub mod service;

pub use error::LeaseError;
pub use lease::LeaseItem;
pub use lessor::Lessor;
pub use service::LeaseService;

pub mod cfg {
    use std::time::Duration;

    pub const DEFAULT_ALIVE_TIME: Duration = Duration::from_secs(2);
}
