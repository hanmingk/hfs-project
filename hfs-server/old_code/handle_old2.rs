// use std::{
//     collections::VecDeque,
//     sync::{Arc, Mutex},
// };

// use bytes::{Buf, BytesMut};
// use tokio::{
//     io::AsyncReadExt,
//     net::tcp::{OwnedReadHalf, OwnedWriteHalf},
// };

// use super::{
//     checksum::CRC32,
//     input::HFSInputStream,
//     packet::{Packet, PacketAck, Status, PACKET_PREFIX},
// };

// type AckQueue = Arc<Mutex<VecDeque<PacketAck>>>;

// pub struct ReceiveHandle {
//     input_stream: HFSInputStream,
//     buffer: BytesMut,
//     up_orh: OwnedReadHalf,
//     down_owh: Option<OwnedWriteHalf>,
//     ack_queue: Option<AckQueue>,
//     crc: Option<CRC32>,
//     chunks_len: usize,
// }

// impl ReceiveHandle {
//     pub async fn run(&mut self) -> crate::HFSResult<()> {
//         loop {
//             let Some(mut pkt) = self.read_packet().await? else {
//                 return Ok(());
//             };

//             match &mut self.down_owh {
//                 Some(down_owh) => {
//                     pkt.write_to(down_owh).await?;
//                     self.input_stream.to_disk(&pkt).await?;
//                 }
//                 None => {
//                     let Some(ack_queue) = &self.ack_queue else {
//                         return Err("ReceiveHandle: lack ack queue".into());
//                     };
//                     let mut ack_queue = ack_queue.lock().unwrap();
//                     let Some(crc) = &self.crc else {
//                         return Err("ReceiveHandle: lack crc".into());
//                     };
//                     let ack = pkt.get_ack(crc, self.chunks_len);
//                     if let Status::Success = &ack.status {
//                         self.input_stream.to_disk(&pkt).await?;
//                     }
//                     ack_queue.push_back(ack);
//                 }
//             }

//             if pkt.last_pkt() {
//                 return Ok(());
//             }
//         }
//     }

//     pub async fn read_packet(&mut self) -> crate::HFSResult<Option<Packet>> {
//         let mut maybe_pkt: Option<Packet> = None;
//         loop {
//             match &mut maybe_pkt {
//                 Some(pkt) => {
//                     let pkt_size = pkt.pkt_size();
//                     if self.buffer.remaining() >= pkt_size {
//                         pkt.loading_transfer_data(&self.buffer[..pkt_size]);
//                         self.buffer.advance(pkt_size);
//                         return Ok(maybe_pkt);
//                     }
//                 }
//                 None => match self.parse_header().await {
//                     Ok(pkt) => maybe_pkt = Some(pkt),
//                     Err(Error::Incomplete) => {}
//                     Err(e) => return Err(e.into()),
//                 },
//             }

//             if 0 == self.up_orh.read_buf(&mut self.buffer).await? {
//                 if self.buffer.is_empty() {
//                     return Ok(None);
//                 } else {
//                     return Err("Connection reset by peer".into());
//                 }
//             }
//         }
//     }

//     async fn parse_header(&mut self) -> Result<Packet, Error> {
//         if self.buffer.remaining() < Packet::pkt_header_len() {
//             return Err(Error::Incomplete);
//         }

//         for i in 0..PACKET_PREFIX.len() {
//             if self.buffer.get_u8() != PACKET_PREFIX[i] {
//                 return Err(Error::UnknownHeader);
//             }
//         }

//         let last_pkt = self.buffer.get_u8() != 0;
//         let seq_id = self.buffer.get_u32();
//         let offset = self.buffer.get_u64();
//         let pkt_len = self.buffer.get_u64();
//         let csum_len = self.buffer.get_u64();

//         Ok(Packet::with_header(
//             seq_id,
//             offset,
//             csum_len as usize,
//             pkt_len as usize,
//             last_pkt,
//         ))
//     }
// }

// #[derive(Debug)]
// pub enum Error {
//     Incomplete,
//     UnknownHeader,
//     Other(crate::Error),
// }

// impl std::error::Error for Error {}

// impl std::fmt::Display for Error {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Error::Incomplete => "Stream ended early".fmt(f),
//             Error::UnknownHeader => "Unknown packet header".fmt(f),
//             Error::Other(e) => e.fmt(f),
//         }
//     }
// }
