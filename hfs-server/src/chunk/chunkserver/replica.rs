use std::{
    io::{Cursor, SeekFrom},
    path::Path,
};

use bytes::Buf;
use hfs_proto::chunk::Status;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    chunk::cfg::{DEFAULT_REPLICA_SIZE, DIR_INDEX_NAME},
    transport::{
        cfg::{PACKET_CHECKSUM_SIZE, PACKET_CHUNKS_SIZE},
        replica_file::VERSION_POS,
    },
};

#[derive(Debug, Clone)]
pub struct ReplicaMetaInfo {
    pub chunk_id: i64,
    pub replica_id: i64,
    pub num_bytes: u32,
    pub size: u32,
    pub version: u32,
    pub status: Status,
}

impl ReplicaMetaInfo {
    pub async fn new(
        base_dir: &str,
        chunk_id: i64,
        replica_id: i64,
    ) -> Result<ReplicaMetaInfo, Error> {
        let meta_info = ReplicaMetaInfo {
            chunk_id,
            replica_id,
            num_bytes: 0,
            size: DEFAULT_REPLICA_SIZE as u32,
            version: 0,
            status: Status::Complete,
        };

        let data_path = data_dir(base_dir, replica_id);
        let std_path = Path::new(&data_path);
        if let Some(parent_path) = std_path.parent() {
            if !parent_path.exists() {
                let _ = std::fs::create_dir_all(parent_path);
            }
        }

        let data_file = File::create(data_path).await?;
        data_file.set_len(DEFAULT_REPLICA_SIZE as u64).await?;

        let size = (DEFAULT_REPLICA_SIZE / PACKET_CHUNKS_SIZE * PACKET_CHECKSUM_SIZE) as u64;
        let mut meta_file = File::create(meta_dir(base_dir, replica_id)).await?;
        meta_file.set_len(size).await?;

        meta_file.seek(SeekFrom::Start(size)).await?;
        let mut history_info = vec![meta_info];
        history_to(&mut meta_file, &history_info).await?;

        Ok(history_info.remove(0))
    }

    pub async fn from_file(base_dir: &str, replica_id: i64) -> Option<Self> {
        let meta_dir = meta_dir(base_dir, replica_id);

        let mut meta_file = File::open(meta_dir).await.ok()?;
        meta_file.seek(SeekFrom::Start(VERSION_POS)).await.ok()?;

        let history_info = history_from(&mut meta_file).await.ok()?;

        history_info.last().cloned()
    }

    pub fn data_dir(&self, base_dir: &str) -> String {
        data_dir(base_dir, self.replica_id)
    }

    pub fn meta_dir(&self, base_dir: &str) -> String {
        meta_dir(base_dir, self.replica_id)
    }

    pub async fn serialize<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        let mut buffer = [0u8; 29];

        buffer[0..8].copy_from_slice(&self.chunk_id.to_be_bytes());
        buffer[8..16].copy_from_slice(&self.replica_id.to_be_bytes());
        buffer[16..20].copy_from_slice(&self.num_bytes.to_be_bytes());
        buffer[20..24].copy_from_slice(&self.size.to_be_bytes());
        buffer[24..28].copy_from_slice(&self.version.to_be_bytes());

        // buffer[28] = #
        buffer[28] = 35;

        if writer.write_all(&buffer).await.is_err() {
            return Err(Error::SerializeErr);
        }

        writer.flush().await?;

        Ok(())
    }

    pub async fn deserialize<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, Error> {
        let mut buffer = [0u8; 29];

        if reader.read_exact(&mut buffer).await.is_err() {
            return Err(Error::DeserializeErr);
        };

        if buffer[28] != 35 {
            return Err(Error::DeserializeErr);
        }

        Ok(ReplicaMetaInfo {
            chunk_id: i64::from_be_bytes(buffer[0..8].try_into().unwrap()),
            replica_id: i64::from_be_bytes(buffer[8..16].try_into().unwrap()),
            num_bytes: u32::from_be_bytes(buffer[16..20].try_into().unwrap()),
            size: u32::from_be_bytes(buffer[20..24].try_into().unwrap()),
            version: u32::from_be_bytes(buffer[24..28].try_into().unwrap()),
            status: Status::Complete,
        })
    }
}

pub const HISTORY_PREFIX: [u8; 14] = *b"HistoryReplica";

pub async fn history_from<S: AsyncReadExt + Unpin>(
    stream: &mut S,
) -> Result<Vec<ReplicaMetaInfo>, Error> {
    let mut header_buf = vec![0; HISTORY_PREFIX.len() + 1];

    stream.read_exact(&mut header_buf).await?;

    if &header_buf[..HISTORY_PREFIX.len()] != &HISTORY_PREFIX {
        return Err(Error::Other("History prefix error".into()));
    }

    let len = header_buf[HISTORY_PREFIX.len()];
    let mut info_list = vec![];
    for _ in 0..len {
        info_list.push(ReplicaMetaInfo::deserialize(stream).await?);
    }

    Ok(info_list)
}

pub async fn history_to<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    history_list: &Vec<ReplicaMetaInfo>,
) -> Result<(), Error> {
    let total = HISTORY_PREFIX.len() + 1 + history_list.len() * 29;
    let mut cur_buf = Cursor::new(vec![0u8; total]);

    cur_buf.write_all(&HISTORY_PREFIX).await?;
    cur_buf.write_u8(history_list.len() as u8).await?;

    for item in history_list {
        item.serialize(&mut cur_buf).await?;
    }

    cur_buf.set_position(0);

    stream.write_all(cur_buf.chunk()).await?;

    stream.flush().await?;

    Ok(())
}

pub fn data_dir(base_dir: &str, replica_id: i64) -> String {
    let mut base_dir = id_to_dir(base_dir, replica_id);

    base_dir.push_str("/chunk_");
    base_dir.push_str(&replica_id.to_string());

    base_dir
}

pub fn meta_dir(base_dir: &str, replica_id: i64) -> String {
    let mut base_dir = id_to_dir(base_dir, replica_id);

    base_dir.push_str("/chunk_");
    base_dir.push_str(&replica_id.to_string());
    base_dir.push_str(".meta");

    base_dir
}

pub fn id_to_dir(base_dir: &str, chunk_id: i64) -> String {
    // First-level directory index
    let d1 = ((chunk_id >> 16) & 0x1f) as u32;
    // Second-level directory index
    let d2 = ((chunk_id >> 8) & 0x1f) as u32;

    format!(
        "{}/data/{}{}/{}{}",
        base_dir, DIR_INDEX_NAME, d1, DIR_INDEX_NAME, d2
    )
}

#[derive(Debug)]
pub enum Error {
    SerializeErr,
    DeserializeErr,
    Other(crate::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Other(value.into())
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SerializeErr => "ReplicaMetaInfo: serialize error".fmt(f),
            Error::DeserializeErr => "ReplicaMetaInfo: deserialize error".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}
