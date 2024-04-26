use volo_grpc::Status;

pub fn volo_status(msg: &str) -> Status {
    Status::new(volo_grpc::Code::Unknown, msg)
}
