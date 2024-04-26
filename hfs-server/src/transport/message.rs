use std::{
    io::Cursor,
    net::{Ipv4Addr, SocketAddr},
};

use bytes::Buf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub struct PipelineBuildMsg {
    conn_info: Vec<ConnInfo>,
}

impl PipelineBuildMsg {
    pub fn new(conn_info: Vec<ConnInfo>) -> PipelineBuildMsg {
        PipelineBuildMsg { conn_info }
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        if !src.has_remaining() {
            return Err(Error::InComplete);
        }
        let info_len = src.get_u8();
        for _ in 0..info_len {
            ConnInfo::check(src)?
        }
        Ok(())
    }

    pub async fn serialize<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        if self.conn_info.len() > u8::MAX as usize {
            return Err(Error::OverMaxNodes);
        }

        writer.write_u8(self.conn_info.len() as u8).await?;

        for item in &self.conn_info {
            item.serialize(writer).await?;
        }

        writer.flush().await?;
        Ok(())
    }

    pub async fn deserialize<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, Error> {
        let info_len = reader.read_u8().await? as usize;

        let mut conn_info = Vec::with_capacity(info_len);

        for _ in 0..info_len {
            conn_info.push(ConnInfo::deserialize(reader).await?);
        }

        Ok(PipelineBuildMsg { conn_info })
    }

    pub fn take_conn(&mut self) -> ConnInfo {
        self.conn_info.remove(0)
    }

    pub fn next_conn_addr(&self) -> Option<SocketAddr> {
        self.conn_info.get(0).map(|info| info.to_socket_addr())
    }

    // pub fn new_test() -> PipelineBuildMsg {
    //     let meta_info = MetaInfo {
    //         csum_len: 1,
    //         chunks_len: 1,
    //         replica_id: 2,
    //     };
    //     let conn_info = vec![
    //         ConnInfo {
    //             ipv4: [127, 0, 0, 1],
    //             port: 3080,
    //             meta_info,
    //         },
    //         ConnInfo {
    //             ipv4: [127, 0, 0, 1],
    //             port: 4080,
    //             meta_info,
    //         },
    //         ConnInfo {
    //             ipv4: [127, 0, 0, 1],
    //             port: 5080,
    //             meta_info,
    //         },
    //     ];

    //     PipelineBuildMsg { conn_info }
    // }
}

#[derive(Debug)]
pub struct ConnInfo {
    pub ipv4: [u8; 4],
    pub port: u16,
    pub meta_info: MetaInfo,
}

impl ConnInfo {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        if src.remaining() < 6 {
            return Err(Error::InComplete);
        }
        src.advance(6);
        MetaInfo::check(src)?;
        Ok(())
    }

    async fn serialize<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        let mut buffer = [0u8; 6];

        buffer[0..4].copy_from_slice(&self.ipv4);
        buffer[4..6].copy_from_slice(&self.port.to_be_bytes());

        writer.write_all(&buffer).await?;

        self.meta_info.serialize(writer).await?;

        Ok(())
    }

    async fn deserialize<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, Error> {
        let mut buffer = [0u8; 6];

        reader.read_exact(&mut buffer).await?;

        Ok(ConnInfo {
            ipv4: buffer[0..4].try_into().unwrap(),
            port: u16::from_be_bytes(buffer[4..6].try_into().unwrap()),
            meta_info: MetaInfo::deserialize(reader).await?,
        })
    }

    pub fn to_socket_addr(&self) -> SocketAddr {
        let ip_addr = Ipv4Addr::from(self.ipv4);
        SocketAddr::new(ip_addr.into(), self.port)
    }

    pub fn into_inner(self) -> MetaInfo {
        self.meta_info
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetaInfo {
    pub csum_len: u16,
    pub chunks_len: u16,
    pub replica_id: i64,
}

impl MetaInfo {
    fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        if src.remaining() < 12 {
            return Err(Error::InComplete);
        }
        src.advance(12);
        Ok(())
    }

    async fn serialize<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<(), Error> {
        let mut buffer = [0u8; 12];

        buffer[0..2].copy_from_slice(&self.csum_len.to_be_bytes());
        buffer[2..4].copy_from_slice(&self.chunks_len.to_be_bytes());
        buffer[4..12].copy_from_slice(&self.replica_id.to_be_bytes());

        writer.write_all(&mut buffer).await?;
        Ok(())
    }

    async fn deserialize<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, Error> {
        let mut buffer = [0u8; 12];

        reader.read_exact(&mut buffer).await?;

        Ok(MetaInfo {
            csum_len: u16::from_be_bytes(buffer[0..2].try_into().unwrap()),
            chunks_len: u16::from_be_bytes(buffer[2..4].try_into().unwrap()),
            replica_id: i64::from_be_bytes(buffer[4..12].try_into().unwrap()),
        })
    }
}

#[derive(Debug)]
pub enum Error {
    InComplete,
    OverMaxNodes,
    Other(crate::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InComplete => "Not enough data is available to parse a message".fmt(f),
            Error::OverMaxNodes => "Over u8::MAX pipeline number of nodes".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Other(value.into())
    }
}
