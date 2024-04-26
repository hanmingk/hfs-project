use std::io::SeekFrom;

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};

#[derive(Debug)]
pub struct Replica {
    src: String,
    replica_id: u64,
    num_bytes: u32,
    replica_size: u32,
    version: u32,
    data_file: File,
    meta_file: File,
}

impl Replica {
    pub async fn new() {
        // let mut rpa_file = File::open("path").await.unwrap();
        // rpa_file.seek(SeekFrom::Start());
    }
}
