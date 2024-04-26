use std::io::SeekFrom;

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::chunk::{
    cfg::DEFAULT_REPLICA_SIZE,
    chunkserver::replica::{self, history_from, history_to, ReplicaMetaInfo},
};

use super::{
    cfg::{DEFAULT_PACKET_SIZE, PACKET_CHECKSUM_SIZE, PACKET_CHUNKS_SIZE},
    checksum::CRC32,
    packet::Packet,
};

pub const VERSION_POS: u64 =
    (DEFAULT_REPLICA_SIZE / PACKET_CHUNKS_SIZE * PACKET_CHECKSUM_SIZE) as u64;

#[derive(Debug)]
pub struct ReplicaFile {
    meta_info: ReplicaMetaInfo,
    data_file: File,
    meta_file: File,
}

impl ReplicaFile {
    pub fn replica_id(&self) -> i64 {
        self.meta_info.replica_id
    }

    pub fn chunk_id(&self) -> i64 {
        self.meta_info.chunk_id
    }

    pub async fn from(base_dir: &str, metainfo: ReplicaMetaInfo) -> crate::HFSResult<ReplicaFile> {
        let data_file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(&metainfo.data_dir(base_dir))
            .await?;
        let meta_file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(&metainfo.meta_dir(base_dir))
            .await?;

        let mut replica_file = ReplicaFile {
            meta_info: metainfo,
            data_file,
            meta_file,
        };

        replica_file.reset_cursor().await?;

        Ok(replica_file)
    }

    pub async fn write_pkt(&mut self, pkt: &Packet, crc: &CRC32) -> Result<(), Error> {
        if self.meta_info.num_bytes + pkt.data_len() as u32 > self.meta_info.size {
            return Err(Error::Full);
        }

        if pkt.offset() != self.meta_info.num_bytes as u64 {
            return Err(Error::ErrPacket);
        }

        let num_chunks = self.meta_info.num_bytes as usize / PACKET_CHUNKS_SIZE;
        let chunks_offset = self.meta_info.num_bytes as usize % PACKET_CHUNKS_SIZE;

        if chunks_offset == 0 {
            self.data_file.write_all(pkt.data()).await?;
            self.meta_info.num_bytes += pkt.data().len() as u32;
            self.meta_file.write_all(pkt.checksum()).await?;
        } else {
            let mut offset_buf = vec![0; chunks_offset];
            let data_buf = pkt.data();

            self.data_file
                .seek(SeekFrom::Start((num_chunks * PACKET_CHUNKS_SIZE) as u64))
                .await?;
            self.data_file.read_exact(&mut offset_buf).await?;

            self.data_file.write_all(data_buf).await?;
            self.meta_info.num_bytes += data_buf.len() as u32;

            offset_buf.extend_from_slice(&data_buf[..PACKET_CHUNKS_SIZE - chunks_offset]);

            self.meta_file
                .seek(SeekFrom::Current(-(PACKET_CHECKSUM_SIZE as i64)))
                .await?;
            self.meta_file.write_u32(crc.checksum(&offset_buf)).await?;

            for chunk in data_buf.chunks(PACKET_CHUNKS_SIZE) {
                self.meta_file.write_u32(crc.checksum(chunk)).await?;
            }
        }

        self.data_file.flush().await?;
        self.meta_file.flush().await?;

        Ok(())
    }

    pub async fn history_info(&mut self) -> Result<Vec<ReplicaMetaInfo>, Error> {
        self.meta_file.seek(SeekFrom::Start(VERSION_POS)).await?;

        let history_info = history_from(&mut self.meta_file).await?;

        self.reset_cursor().await?;

        Ok(history_info)
    }

    pub async fn packets(&mut self) -> ReplicaPacket {
        self.data_file.seek(SeekFrom::Start(0)).await.unwrap();
        self.meta_file.seek(SeekFrom::Start(0)).await.unwrap();

        let body_size = DEFAULT_PACKET_SIZE - Packet::pkt_header_len();
        let chunk_size = PACKET_CHECKSUM_SIZE + PACKET_CHUNKS_SIZE;
        let pkt_max_chunks = 1.max(body_size / chunk_size);
        let data_len = (pkt_max_chunks * PACKET_CHUNKS_SIZE) as u64;

        ReplicaPacket {
            chunk_size,
            pkt_max_chunks,
            data_len,
            cursor: 0,
            lenght: self.meta_info.num_bytes as u64,
            data_file: &mut self.data_file,
            meta_file: &mut self.meta_file,
        }
    }

    pub async fn update_version(&mut self) -> Result<(), Error> {
        self.meta_file.seek(SeekFrom::Start(VERSION_POS)).await?;

        let mut history_list = history_from(&mut self.meta_file).await?;

        self.meta_info.version += 1;
        history_list.push(self.meta_info.clone());
        if history_list.len() > 7 {
            history_list.remove(0);
        }

        self.meta_file.seek(SeekFrom::Start(VERSION_POS)).await?;

        history_to(&mut self.meta_file, &history_list).await?;

        Ok(())
    }

    pub async fn reset_history(&mut self, history_info: Vec<ReplicaMetaInfo>) -> Result<(), Error> {
        if history_info.is_empty() {
            return Err(Error::Other("Empty history info".into()));
        }

        self.meta_info = history_info.last().unwrap().clone();

        self.meta_file.seek(SeekFrom::Start(VERSION_POS)).await?;

        history_to(&mut self.meta_file, &history_info).await?;

        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<Option<ReplicaMetaInfo>, Error> {
        let mut history_info = history_from(&mut self.meta_file).await?;

        history_info.pop();

        let last_info = history_info.last().map(|r| r.clone());

        if let Some(info) = &last_info {
            let num = info.num_bytes / PACKET_CHUNKS_SIZE as u32;
            let fmt_pos = num * PACKET_CHUNKS_SIZE as u32;

            if fmt_pos != info.num_bytes {
                let seek1 = num * PACKET_CHECKSUM_SIZE as u32;
                self.meta_file.seek(SeekFrom::Start(seek1 as u64)).await?;

                let seek2 = num * PACKET_CHUNKS_SIZE as u32;
                self.data_file.seek(SeekFrom::Start(seek2 as u64)).await?;

                let mut buf = vec![0u8; (info.num_bytes - seek2) as usize];
                self.data_file.read_exact(&mut buf).await?;

                let crc = CRC32::new();
                let checksum = crc.checksum(&buf);

                self.meta_file.write_u32(checksum).await?;

                self.meta_file.seek(SeekFrom::Start(0)).await?;
                self.data_file.seek(SeekFrom::Start(0)).await?;
            }
        }

        if last_info.is_some() {
            self.reset_history(history_info).await?;
        }

        Ok(last_info)
    }

    pub async fn reset_cursor(&mut self) -> Result<(), Error> {
        self.data_file
            .seek(SeekFrom::Start(self.meta_info.num_bytes as u64))
            .await?;

        let num_chunks =
            (self.meta_info.num_bytes as usize + PACKET_CHUNKS_SIZE - 1) / PACKET_CHUNKS_SIZE;
        self.meta_file
            .seek(SeekFrom::Start((num_chunks * PACKET_CHECKSUM_SIZE) as u64))
            .await?;

        Ok(())
    }

    pub async fn reset(&mut self) -> Result<(), Error> {
        self.meta_info.num_bytes = 0;
        self.meta_info.version = 0;

        self.data_file.seek(SeekFrom::Start(0)).await?;
        self.meta_file.seek(SeekFrom::Start(0)).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ReplicaPacket<'a> {
    chunk_size: usize,
    pkt_max_chunks: usize,
    data_len: u64,
    cursor: u64,
    lenght: u64,
    data_file: &'a mut File,
    meta_file: &'a mut File,
}

impl ReplicaPacket<'_> {
    pub async fn next(&mut self) -> Option<Packet> {
        if self.cursor == self.lenght {
            return None;
        }

        let sizes = if self.lenght - self.cursor > self.data_len {
            (
                self.pkt_max_chunks * PACKET_CHECKSUM_SIZE,
                self.pkt_max_chunks * self.chunk_size,
            )
        } else {
            let remaining_len = (self.lenght - self.cursor) as usize;
            let num_chunk = (remaining_len + PACKET_CHUNKS_SIZE - 1) / PACKET_CHUNKS_SIZE;
            let csum_len = num_chunk * PACKET_CHECKSUM_SIZE;

            (csum_len, csum_len + remaining_len)
        };

        let seq_id = self.cursor / self.data_len;
        let mut pkt = Packet::new(seq_id as u32, self.cursor, sizes.0, sizes.1);

        if let Err(_) = pkt.loading_all_data(&mut self.data_file).await {
            return None;
        }
        if let Err(_) = pkt.loading_all_checksum(&mut self.meta_file).await {
            return None;
        }

        self.cursor += (sizes.1 - sizes.0) as u64;

        if self.cursor == self.lenght {
            pkt.set_last_pkt(true);
        }

        Some(pkt)
    }
}

#[derive(Debug)]
pub enum Error {
    Full,
    ErrPacket,
    Other(crate::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Other(value.into())
    }
}

impl From<replica::Error> for Error {
    fn from(value: replica::Error) -> Self {
        Error::Other(value.into())
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Full => "Replica file is full".fmt(f),
            Error::ErrPacket => "Packet error".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}
