use std::sync::Arc;

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{self, Duration},
};

use crate::{
    chunk::chunkserver::manager::{update_complete, ReplicaManager},
    transport::message::PipelineBuildMsg,
};

use super::task::{copy_task, download_task, upload_task};

#[derive(Debug)]
pub struct TransportService {
    pub replica_mgr: Arc<ReplicaManager>,
    pub listener: TcpListener,
    pub shutdown_complete: mpsc::Sender<()>,
}

impl TransportService {
    pub async fn run(&mut self) -> crate::HFSResult<()> {
        loop {
            let socket = self.accept().await?;
            let replica_mgr = self.replica_mgr.clone();
            let shutdown_complete = self.shutdown_complete.clone();

            // Spawn a new task to process the connections. Tokio tasks are like
            // asynchronous green threads and are executed concurrently.
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handle_task(socket, replica_mgr, shutdown_complete).await {
                    tracing::error!(upload = ?err, "transport falid");
                }
            });
        }
    }

    async fn accept(&mut self) -> crate::HFSResult<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

async fn handle_task(
    mut socket: TcpStream,
    replica_mgr: Arc<ReplicaManager>,
    _shutdown_complete: mpsc::Sender<()>,
) -> crate::HFSResult<()> {
    let op = TransportOp::from_socket(&mut socket).await?;

    match op {
        TransportOp::Upload => {
            let mut pbm = PipelineBuildMsg::deserialize(&mut socket).await?;

            let conn_info = pbm.take_conn();
            let replica_id = conn_info.into_inner().replica_id;

            let mut rollback = false;

            tracing::info!("upload data to replica = {} ...", replica_id);

            if let Err(err) = upload_task(socket, replica_id, pbm, &replica_mgr).await {
                rollback = true;
                tracing::error!(upload_task = %err, "upload error, rollback...");
            } else {
                tracing::info!("upload data to replica = {} success", replica_id);
            }

            update_complete(replica_id, &replica_mgr, rollback).await;

            Ok(())
        }
        TransportOp::Download => download_task(socket, &replica_mgr).await,
        TransportOp::Copy => copy_task(socket, &replica_mgr).await,
    }
}

#[derive(Debug)]
pub enum TransportOp {
    Upload,
    Download,
    Copy,
}

impl TransportOp {
    pub fn from_bytes(bytes: &[u8]) -> Option<TransportOp> {
        if bytes.len() < 12 {
            return None;
        }

        if &bytes[0..11] != b"TransportOp" {
            return None;
        }

        let op = match bytes[11] {
            1 => TransportOp::Upload,
            2 => TransportOp::Download,
            3 => TransportOp::Copy,
            _ => return None,
        };

        Some(op)
    }

    pub fn to_bytes(&self) -> [u8; 12] {
        let mut bytes = [84, 114, 97, 110, 115, 112, 111, 114, 116, 79, 112, 0];
        match self {
            TransportOp::Upload => bytes[11] = 1,
            TransportOp::Download => bytes[11] = 2,
            TransportOp::Copy => bytes[11] = 3,
        }

        bytes
    }

    pub async fn from_socket(socket: &mut TcpStream) -> crate::HFSResult<TransportOp> {
        let mut bytes = [0; 12];
        let len = socket.read_exact(&mut bytes).await?;
        if len < bytes.len() {
            return Err("Error opcode".into());
        }
        match TransportOp::from_bytes(&bytes) {
            Some(op) => Ok(op),
            None => Err("Error opcode".into()),
        }
    }
}

// #[cfg(test)]
// mod test {
//     use std::io::Cursor;

//     use bytes::{Buf, BytesMut};
//     use tokio::{
//         fs::File,
//         io::{AsyncReadExt, BufWriter},
//         net::TcpListener,
//         sync::{broadcast, mpsc},
//     };

//     use crate::transport::{
//         checksum::CRC32,
//         chunkserver::{service::TransportService, upload::connect_upload},
//         message::PipelineBuildMsg,
//         packet::{Error, Packet, PacketAck},
//         utils::stream_read_buf,
//     };

//     #[tokio::test]
//     async fn test1() {
//         tracing_subscriber::fmt::init();
//         let (notify_shutdown, _) = broadcast::channel::<()>(1);
//         let listener = TcpListener::bind("127.0.0.1:3080").await.unwrap();
//         let (shutdown_complete_tx, _) = mpsc::channel(1);
//         let mut s = TransportService {
//             listener,
//             shutdown_complete_tx,
//         };
//         s.run(&notify_shutdown).await.unwrap();
//     }

//     #[tokio::test]
//     async fn test2() {
//         tracing_subscriber::fmt::init();
//         let (notify_shutdown, _) = broadcast::channel::<()>(1);
//         let listener = TcpListener::bind("127.0.0.1:4080").await.unwrap();
//         let (shutdown_complete_tx, _) = mpsc::channel(1);
//         let mut s = TransportService {
//             listener,
//             shutdown_complete_tx,
//         };
//         s.run(&notify_shutdown).await.unwrap();
//     }

//     #[tokio::test]
//     async fn test3() {
//         tracing_subscriber::fmt::init();
//         let (notify_shutdown, _) = broadcast::channel::<()>(1);
//         let listener = TcpListener::bind("127.0.0.1:5080").await.unwrap();
//         let (shutdown_complete_tx, _) = mpsc::channel(1);
//         let mut s = TransportService {
//             listener,
//             shutdown_complete_tx,
//         };
//         s.run(&notify_shutdown).await.unwrap();
//     }

//     #[tokio::test]
//     async fn test4() {
//         let pbm = PipelineBuildMsg::new_test();
//         let socket = connect_upload(pbm).await.unwrap();

//         let (mut orh, owh) = socket.into_split();

//         let mut write_stream = BufWriter::new(owh);

//         let ack_handle = tokio::spawn(async move {
//             let mut buffer = BytesMut::new();

//             loop {
//                 let mut buf = Cursor::new(buffer.chunk());
//                 let mut last_pkt_pos = 0;
//                 loop {
//                     match PacketAck::check(&mut buf) {
//                         Ok(_) => {
//                             buf.set_position(last_pkt_pos);
//                             match PacketAck::parse(&mut buf) {
//                                 Ok(pkt_ack) => {
//                                     println!(
//                                         "packet{} status: {:?}",
//                                         pkt_ack.seq_id, pkt_ack.status
//                                     );
//                                     if pkt_ack.is_last {
//                                         println!("All ack has get, shutdown");
//                                         return;
//                                     }
//                                 }
//                                 Err(_) => println!("Unknown pkt ack"),
//                             }
//                             last_pkt_pos = buf.position();
//                         }
//                         Err(Error::InComplete) => break,
//                         Err(err) => {
//                             println!("ack check error: {}", err);
//                             return;
//                         }
//                     }
//                 }

//                 if last_pkt_pos != 0 {
//                     buffer.advance(last_pkt_pos as usize);
//                 }

//                 if !stream_read_buf(&mut orh, &mut buffer).await.unwrap() {
//                     return;
//                 };
//             }
//         });

//         let mut buffer = BytesMut::new();

//         let mut file = File::open("/home/hmk/code/rust/hfs-project/test_data/test.txt")
//             .await
//             .unwrap();

//         let csum_len = 126 * 4;
//         let pkt_size = 126 * 516;
//         let crc = CRC32::new();

//         let mut cur_seq_id: u64 = 0;

//         'outer: loop {
//             let offset = (126 * 512) * cur_seq_id;
//             let mut pkt = Packet::new(cur_seq_id as u32, offset, csum_len, pkt_size);
//             'outer1: loop {
//                 loop {
//                     if buffer.remaining() < 512 {
//                         break;
//                     }
//                     match pkt.loading_data(&crc, &buffer[..512]) {
//                         Ok(_) => buffer.advance(512),
//                         Err(_) => {
//                             pkt.write_to(&mut write_stream).await.unwrap();
//                             println!("Packet{} send", cur_seq_id);
//                             break 'outer1;
//                         }
//                     }
//                 }

//                 if 0 == file.read_buf(&mut buffer).await.unwrap() {
//                     if !buffer.is_empty() {
//                         pkt.loading_data(&crc, &buffer[..]).unwrap();
//                         pkt.set_last_pkt(true);
//                         pkt.write_to(&mut write_stream).await.unwrap();
//                         println!("Last pkt send");
//                     }
//                     break 'outer;
//                 }
//             }
//             cur_seq_id += 1;
//         }

//         ack_handle.await.unwrap();
//     }
// }
