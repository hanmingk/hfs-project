use std::io::Cursor;

use bytes::Buf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    cfg::{PACKET_CHECKSUM_SIZE, PACKET_CHUNKS_SIZE},
    checksum::CRC32,
};

pub const PACKET_PREFIX: [u8; 3] = *b"\n$\r";

#[derive(Debug)]
pub enum Error {
    InComplete,
    PacketOverflow,
    ChecksumOverflow,
    EmptyPacket,
    CorruptPacket,
}

#[derive(Debug)]
pub struct Packet {
    seq_id: u32,
    offset: u64,
    csum_start: usize,
    csum_pos: usize,
    data_start: usize,
    data_pos: usize,
    buffer: Vec<u8>,
    last_pkt: bool,
}

#[derive(Debug)]
pub struct PacketAck {
    pub seq_id: u32,
    pub offset: u64,
    pub is_last: bool,
    pub status: Status,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    Success,
    Error,
    ErrorChecksum,
    InProgress,
}

impl Packet {
    pub fn new(seq_id: u32, offset: u64, csum_len: usize, pkt_size: usize) -> Packet {
        let csum_start = Packet::pkt_header_len() as usize;
        let data_start = csum_start + csum_len;

        Packet {
            seq_id,
            offset,
            csum_start,
            csum_pos: csum_start,
            data_start,
            data_pos: data_start,
            buffer: vec![0; csum_start + pkt_size],
            last_pkt: false,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn seq_id(&self) -> u32 {
        self.seq_id
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer[self.data_start..self.data_pos]
    }

    pub fn checksum(&self) -> &[u8] {
        &self.buffer[self.csum_start..self.csum_pos]
    }

    pub fn pkt_header_len() -> usize {
        // prefix_len, lask_pkt, seq_id, offset, pkt_len, checksum_len
        PACKET_PREFIX.len() + 1 + 4 + 8 + 8 + 8
    }

    pub fn check<B: Buf>(buf: &mut B) -> Result<(), Error> {
        if buf.remaining() < Packet::pkt_header_len() {
            return Err(Error::InComplete);
        }
        let get_prefix = [buf.get_u8(), buf.get_u8(), buf.get_u8()];
        if get_prefix != PACKET_PREFIX {
            return Err(Error::CorruptPacket);
        }
        buf.advance(13);
        let pkt_len = buf.get_u64() as usize;
        if buf.remaining() < pkt_len + 8 {
            return Err(Error::InComplete);
        }
        buf.advance(pkt_len + 8);

        Ok(())
    }

    pub fn parse<B: Buf>(src: &mut B) -> Packet {
        src.advance(3); // skip prefix
        let last_pkt = src.get_u8() != 0;
        let seq_id = src.get_u32();
        let offset = src.get_u64();
        let pkt_len = src.get_u64() as usize;
        let csum_len = src.get_u64() as usize;

        let csum_start = Packet::pkt_header_len();
        let data_start = csum_start + csum_len;
        let mut buffer = vec![0; csum_start + pkt_len];
        buffer[csum_start..].copy_from_slice(&src.chunk()[..pkt_len]);
        src.advance(pkt_len);

        Packet {
            seq_id,
            offset,
            csum_start,
            csum_pos: data_start,
            data_start,
            data_pos: csum_start + pkt_len,
            buffer,
            last_pkt,
        }
    }

    pub fn loading_data(&mut self, crc: &CRC32, bytes: &[u8]) -> Result<(), Error> {
        if self.data_pos + bytes.len() > self.buffer.len() {
            return Err(Error::PacketOverflow);
        }

        let checksum = crc.checksum(bytes).to_be_bytes();
        let csum_end = self.csum_pos + checksum.len();
        self.buffer[self.csum_pos..csum_end].copy_from_slice(&checksum);
        self.csum_pos = csum_end;

        let data_end = self.data_pos + bytes.len();
        self.buffer[self.data_pos..data_end].copy_from_slice(bytes);
        self.data_pos = data_end;
        Ok(())
    }

    pub fn set_last_pkt(&mut self, b: bool) {
        self.last_pkt = b;
    }

    pub fn last_pkt(&self) -> bool {
        self.last_pkt
    }

    pub async fn read_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> crate::HFSResult<Packet> {
        let mut header_buf = vec![0; Packet::pkt_header_len()];

        reader.read_exact(&mut header_buf).await?;

        let last_pkt = header_buf[3] != 0;
        let seq_id = u32::from_be_bytes(header_buf[4..8].try_into().unwrap());
        let offset = u64::from_be_bytes(header_buf[8..16].try_into().unwrap());
        let pkt_len = u64::from_be_bytes(header_buf[16..24].try_into().unwrap()) as usize;
        let csum_len = u64::from_be_bytes(header_buf[24..32].try_into().unwrap()) as usize;

        let csum_start = Packet::pkt_header_len();
        let data_start = csum_start + csum_len;
        let mut buffer = vec![0; csum_start + pkt_len];

        reader.read_exact(&mut buffer[csum_start..]).await?;

        Ok(Packet {
            seq_id,
            offset,
            csum_start,
            csum_pos: data_start,
            data_start,
            data_pos: csum_start + pkt_len,
            buffer,
            last_pkt,
        })
    }

    pub async fn write_to<W: AsyncWriteExt + Unpin>(
        &mut self,
        writer: &mut W,
    ) -> crate::HFSResult<()> {
        if self.data_start == self.data_pos {
            return Err(Error::EmptyPacket.into());
        }

        if self.csum_pos > self.data_start {
            return Err(Error::CorruptPacket.into());
        }

        // write header
        self.buffer[..PACKET_PREFIX.len()].copy_from_slice(&PACKET_PREFIX);
        self.buffer[PACKET_PREFIX.len()] = self.last_pkt as u8;
        self.buffer[4..8].copy_from_slice(&self.seq_id.to_be_bytes());
        self.buffer[8..16].copy_from_slice(&self.offset.to_be_bytes());

        let checksum_len = self.csum_pos - self.csum_start;
        let pkt_len = checksum_len + self.data_pos - self.data_start;
        self.buffer[16..24].copy_from_slice(&pkt_len.to_be_bytes());
        self.buffer[24..32].copy_from_slice(&checksum_len.to_be_bytes());

        if self.csum_pos != self.data_start {
            writer.write_all(&self.buffer[..self.csum_pos]).await?;
            writer
                .write_all(&self.buffer[self.data_start..self.data_pos])
                .await?;
        } else {
            writer.write_all(&self.buffer[..self.data_pos]).await?;
        }

        writer.flush().await?;

        Ok(())
    }

    pub fn check_pkt(&self, crc: &CRC32) -> bool {
        let csum_slice = &self.buffer[self.csum_start..self.csum_pos];
        let data_slice = &self.buffer[self.data_start..self.data_pos];

        if csum_slice.len() % PACKET_CHECKSUM_SIZE != 0 {
            return false;
        }

        let old_checksum = csum_slice
            .chunks(PACKET_CHECKSUM_SIZE)
            .map(|chunk| u32::from_be_bytes(chunk[..].try_into().unwrap()));

        let new_checksum = data_slice
            .chunks(PACKET_CHUNKS_SIZE)
            .map(|chunk| crc.checksum(chunk));

        new_checksum.eq(old_checksum)
    }

    pub fn get_ack(&self, crc: &CRC32) -> PacketAck {
        PacketAck {
            seq_id: self.seq_id,
            offset: self.offset,
            is_last: self.last_pkt,
            status: if self.check_pkt(crc) {
                Status::Success
            } else {
                Status::ErrorChecksum
            },
        }
    }

    pub fn data_len(&self) -> usize {
        self.data_pos - self.data_start
    }

    pub async fn loading_all_data<R: AsyncReadExt + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> Result<(), std::io::Error> {
        reader
            .read_exact(&mut self.buffer[self.data_start..])
            .await?;
        self.data_pos = self.buffer.len();
        Ok(())
    }

    pub async fn loading_all_checksum<R: AsyncReadExt + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> Result<(), std::io::Error> {
        reader
            .read_exact(&mut self.buffer[self.csum_start..self.data_start])
            .await?;
        self.csum_pos = self.data_start;
        Ok(())
    }

    pub fn write_buf(&mut self, buf: &[u8]) -> usize {
        if self.is_full() {
            return 0;
        }

        if self.data_pos + buf.len() > self.buffer.len() {
            let load_len = self.buffer.len() - self.data_pos;
            self.buffer[self.data_pos..].copy_from_slice(&buf[..load_len]);
            self.data_pos += load_len;

            load_len
        } else {
            let end_index = self.data_pos + buf.len();
            self.buffer[self.data_pos..end_index].copy_from_slice(buf);
            self.data_pos = end_index;

            buf.len()
        }
    }

    pub fn free_remaining(&self) -> usize {
        self.buffer.len() - self.data_pos
    }

    pub fn is_full(&self) -> bool {
        self.data_pos == self.buffer.len()
    }

    pub fn gen_checksum(&mut self, crc: &CRC32) {
        let (csum_slice, data_slice) = self.buffer.split_at_mut(self.data_start);
        let data_slice = &mut data_slice[..self.data_pos - self.data_start];

        for chunk in data_slice.chunks(PACKET_CHUNKS_SIZE) {
            let end_index = self.csum_pos + PACKET_CHECKSUM_SIZE;
            csum_slice[self.csum_pos..end_index]
                .copy_from_slice(&crc.checksum(chunk).to_be_bytes());
            self.csum_pos = end_index;
        }
    }
}

impl PacketAck {
    pub fn is_success(&self) -> bool {
        Status::Success == self.status
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        if src.remaining() < 14 {
            return Err(Error::InComplete);
        }

        Ok(())
    }

    pub async fn deserialize<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<PacketAck, Error> {
        let mut buf = [0u8; 14];

        if let Err(_) = reader.read_exact(&mut buf).await {
            return Err(Error::InComplete);
        }

        Ok(PacketAck {
            seq_id: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
            offset: u64::from_be_bytes(buf[4..12].try_into().unwrap()),
            is_last: buf[12] != 0,
            status: Status::from_u8(buf[13])?,
        })
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<PacketAck, Error> {
        Ok(PacketAck {
            seq_id: src.get_u32(),
            offset: src.get_u64(),
            is_last: src.get_u8() != 0,
            status: Status::from_u8(src.get_u8())?,
        })
    }

    pub fn to_bytes(&self) -> [u8; 14] {
        let mut bytes = [0u8; 14];
        bytes[0..4].copy_from_slice(&self.seq_id.to_be_bytes());
        bytes[4..12].copy_from_slice(&self.offset.to_be_bytes());
        bytes[12] = self.is_last as u8;
        bytes[13] = self.status.to_u8();
        bytes
    }
}

impl Status {
    pub fn from_u8(byte: u8) -> Result<Status, Error> {
        let status = match byte {
            1 => Status::Success,
            2 => Status::Error,
            3 => Status::ErrorChecksum,
            4 => Status::InProgress,
            _ => return Err(Error::CorruptPacket),
        };

        Ok(status)
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Status::Success => 1,
            Status::Error => 2,
            Status::ErrorChecksum => 3,
            Status::InProgress => 4,
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::PacketOverflow => "Data packet overflow".fmt(f),
            Error::ChecksumOverflow => "Checksum exceeds scheduled length".fmt(f),
            Error::EmptyPacket => "Empty packet".fmt(f),
            Error::CorruptPacket => "Corrupt packet".fmt(f),
            Error::InComplete => "Not enough data is available to parse a message".fmt(f),
        }
    }
}
