use volo::FastStr;

#[derive(Debug)]
pub enum ClusterError {
    _UnknownServer,
}

impl Into<FastStr> for ClusterError {
    fn into(self) -> FastStr {
        match self {
            ClusterError::_UnknownServer => "Unknown server".into(),
        }
    }
}
