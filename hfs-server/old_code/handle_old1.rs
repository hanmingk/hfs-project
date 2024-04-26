use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bytes::{Buf, BytesMut};
use tokio::{
    io::AsyncReadExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use super::packet::{Packet, PACKET_PREFIX};

type AckQueue = Arc<Mutex<VecDeque<PacketAck>>>;
type PacketQueue = Arc<Mutex<VecDeque<Packet>>>;

#[derive(Debug)]
pub struct PacketRecvHandle {
    up_orh: OwnedReadHalf,
    buffer: BytesMut,
    packet_queue: Option<PacketQueue>,
    ack_queue: Option<AckQueue>,
}

#[derive(Debug)]
pub struct PacketRespHandle {
    down_orh: OwnedWriteHalf,
    packet_queue: PacketQueue,
}

#[derive(Debug)]
pub struct AckRecvHandle {
    up_orh: OwnedReadHalf,
    ack_queue: AckQueue,
}

#[derive(Debug)]
pub struct AckRespHandle {
    down_owh: OwnedWriteHalf,
    ack_queue: AckQueue,
}

#[derive(Debug)]
pub struct PacketAck {
    seq_id: u32,
    offset: u64,
    is_last: bool,
    status: Status,
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    UnknownHeader,
    Other(crate::Error),
}

#[derive(Debug)]
enum Status {
    Success,
    Error,
    ErrorChecknum,
    InProgress,
}

impl PacketRecvHandle {
    pub async fn run(&mut self) {}

    pub async fn read_packet(&mut self) -> crate::HFSResult<Option<Packet>> {
        let mut maybe_pkt: Option<Packet> = None;
        loop {
            match &mut maybe_pkt {
                Some(pkt) => {
                    let pkt_size = pkt.pkt_size();
                    if self.buffer.remaining() >= pkt_size {
                        pkt.loading_transfer_data(&self.buffer[..pkt_size]);
                        self.buffer.advance(pkt_size);
                    }
                }
                None => match self.parse_header().await {
                    Ok(pkt) => maybe_pkt = Some(pkt),
                    Err(Error::Incomplete) => {}
                    Err(e) => return Err(e.into()),
                },
            }

            if 0 == self.up_orh.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("Connection reset by peer".into());
                }
            }
        }
    }

    async fn parse_header(&mut self) -> Result<Packet, Error> {
        if self.buffer.remaining() < Packet::pkt_header_len() {
            return Err(Error::Incomplete);
        }

        for i in 0..PACKET_PREFIX.len() {
            if self.buffer.get_u8() != PACKET_PREFIX[i] {
                return Err(Error::UnknownHeader);
            }
        }

        let last_pkt = self.buffer.get_u8() != 0;
        let seq_id = self.buffer.get_u32();
        let offset = self.buffer.get_u64();
        let pkt_len = self.buffer.get_u64();
        let csum_len = self.buffer.get_u64();

        Ok(Packet::with_header(
            seq_id,
            offset,
            csum_len as usize,
            pkt_len as usize,
            last_pkt,
        ))
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Incomplete => "Stream ended early".fmt(f),
            Error::UnknownHeader => "Unknown packet header".fmt(f),
            Error::Other(e) => e.fmt(f),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    #[test]
    fn test() {
        struct Test {}

        impl Test {
            pub fn new() -> Test {
                tokio::spawn(async {
                    loop {
                        println!("living!!");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
                Test {}
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let test = Test::new();
            thread::sleep(Duration::from_secs(5));
            drop(test);
            println!("Drop test");
            thread::sleep(Duration::from_secs(5));
        })
    }
}
