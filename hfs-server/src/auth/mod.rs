mod error;
mod manager;
mod permissions;
mod service;

pub use error::AuthError;
pub use manager::{auth_deserialize, auth_serialize, AuthManager};
pub use permissions::{OPPermissions, Permissions};
pub use service::AuthService;

pub mod cfg {
    use std::time::Duration;

    pub const DEFAULT_ROOT_NAME: &str = "root";

    pub const DEFAULT_ROOT_PASSWD: &str = "1234";

    pub const MAX_LIVE_TIME: Duration = Duration::from_secs(55);
}
