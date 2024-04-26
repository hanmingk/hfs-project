use std::sync::PoisonError;

use tokio::sync::oneshot::error::RecvError;

#[derive(Debug)]
pub enum DLockError {
    LockedError,
    UnknownLease,
}

impl<T> From<PoisonError<T>> for DLockError {
    fn from(_value: PoisonError<T>) -> Self {
        DLockError::LockedError
    }
}

impl From<RecvError> for DLockError {
    fn from(_value: RecvError) -> Self {
        DLockError::LockedError
    }
}
