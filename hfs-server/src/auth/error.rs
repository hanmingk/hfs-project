#[derive(Debug)]
pub enum AuthError {
    UnSign,
    UserNameExists,
    GroupNameExists,
    PermissionDenied,
    UserNameOrPasswordError,
}

impl std::error::Error for AuthError {}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::UnSign => "not sign".fmt(f),
            AuthError::UserNameExists => "User name exists".fmt(f),
            AuthError::GroupNameExists => "Group name exists".fmt(f),
            AuthError::PermissionDenied => "Permission denied".fmt(f),
            AuthError::UserNameOrPasswordError => "Username or password error".fmt(f),
        }
    }
}

impl From<AuthError> for volo_grpc::Status {
    fn from(value: AuthError) -> Self {
        volo_grpc::Status::new(volo_grpc::Code::Unknown, value.to_string())
    }
}
