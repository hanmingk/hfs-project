use crate::lease::LeaseError;

#[derive(Debug)]
pub enum HFSIoError {
    NoSuchFileOrDirectory,
    ExistsFileOrDirectory,
    FileBlockCorruption,
    FileAlreadyOpen,
    ResourceLockFailed,
    NoPermissions,
    UnSignIn,
}

impl std::error::Error for HFSIoError {}

impl std::fmt::Display for HFSIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HFSIoError::NoSuchFileOrDirectory => "No such file or directory".fmt(f),
            HFSIoError::ExistsFileOrDirectory => "File or directory exists".fmt(f),
            HFSIoError::FileBlockCorruption => "File block corruption".fmt(f),
            HFSIoError::FileAlreadyOpen => "File is already open".fmt(f),
            HFSIoError::ResourceLockFailed => "Resource lock failed".fmt(f),
            HFSIoError::NoPermissions => "Permission denied".fmt(f),
            HFSIoError::UnSignIn => "Not logged in".fmt(f),
        }
    }
}

impl From<HFSIoError> for volo_grpc::Status {
    fn from(value: HFSIoError) -> Self {
        volo_grpc::Status::new(volo_grpc::Code::Unknown, value.to_string())
    }
}

impl From<LeaseError> for HFSIoError {
    fn from(_value: LeaseError) -> Self {
        HFSIoError::ResourceLockFailed
    }
}
