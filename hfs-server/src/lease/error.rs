use tokio::sync::oneshot::error::RecvError;

#[derive(Debug)]
pub enum LeaseError {
    LockedError,
    UnknownLease,
}

impl std::error::Error for LeaseError {}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::LockedError => "Locked error".fmt(f),
            LeaseError::UnknownLease => "Unknown lease".fmt(f),
        }
    }
}

impl From<RecvError> for LeaseError {
    fn from(_value: RecvError) -> Self {
        LeaseError::LockedError
    }
}

impl From<LeaseError> for volo_grpc::Status {
    fn from(value: LeaseError) -> Self {
        volo_grpc::Status::new(volo_grpc::Code::Unknown, value.to_string())
    }
}
