use std::{io::Cursor, net::SocketAddr};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::mpsc::{self, Receiver, Sender},
};

use crate::{
    chunk::chunkserver::manager::ReplicaManager,
    transport::{
        checksum::CRC32,
        message::PipelineBuildMsg,
        packet::{Error, Packet, Status},
        replica_file::ReplicaFile,
        utils::stream_read_buf,
    },
};

use super::service::TransportOp;

const PIPELINE_BUILD_ACK: [u8; 16] = *b"PipelineBuildAck";

pub async fn connect_upload(pbm: PipelineBuildMsg) -> crate::HFSResult<TcpStream> {
    let addr = pbm.next_conn_addr().unwrap();
    let mut socket = connect_with_upload(addr).await.unwrap();

    pbm.serialize(&mut socket).await?;

    // Wait pipeline build ack msg.
    client_check_ack(&mut socket).await?;

    Ok(socket)
}

pub async fn upload_task(
    mut stream: TcpStream,
    replica_id: i64,
    pbm: PipelineBuildMsg,
    replica_mgr: &ReplicaManager,
) -> crate::HFSResult<()> {
    let meta_info = replica_mgr
        .update_replica(&replica_id)
        .ok_or("Update task: unknown replica id")?;
    let replica_file = ReplicaFile::from(&replica_mgr.base_dir, meta_info).await?;

    match pbm.next_conn_addr() {
        Some(addr) => {
            // connect down and send pipeline build msg.
            let mut down_stream = connect_with_upload(addr).await?;
            pbm.serialize(&mut down_stream).await?;

            // waiting for down pipeline build ack.
            waiting_ack(&mut stream, &mut down_stream).await?;

            // The pipeline construction is completed and data uploading begins.
            // Create a packet transmission channel.
            let (pkt_tx, pkt_rx) = mpsc::channel::<Packet>(64);

            // Packet to disk task
            let disk_handle = tokio::spawn(to_disk(pkt_rx, replica_file));

            let (up_orh, up_owh) = stream.into_split();
            let (down_orh, down_owh) = down_stream.into_split();

            // Ack task
            let ack_handle = tokio::spawn(transfer_ack(up_owh, down_orh));

            receive_and_send(up_orh, down_owh, pkt_tx).await?;

            ack_handle.await??;

            disk_handle.await??;
        }
        None => {
            // The last node of pipeline
            // send pipeline build ack
            stream.write_all(&PIPELINE_BUILD_ACK).await?;

            // Create a packet transmission channel.
            let (pkt_tx, pkt_rx) = mpsc::channel::<Packet>(64);

            // Packet to disk task
            let disk_handle = tokio::spawn(to_disk(pkt_rx, replica_file));

            receive_and_resp(stream, pkt_tx).await?;

            disk_handle.await??;
        }
    }

    Ok(())
}

async fn connect_with_upload(addr: SocketAddr) -> crate::HFSResult<TcpStream> {
    let mut socket = TcpStream::connect(addr).await?;
    let opcode = TransportOp::Upload;
    socket.write_all(&opcode.to_bytes()).await?;
    Ok(socket)
}

async fn client_check_ack(stream: &mut TcpStream) -> crate::HFSResult<()> {
    let mut buf = [0u8; 16];
    stream.read_exact(&mut buf).await?;
    if buf != PIPELINE_BUILD_ACK {
        return Err("Pipline build ack error".into());
    }

    Ok(())
}

/// Wait for pipeline build success ack
async fn waiting_ack(
    up_stream: &mut TcpStream,
    down_stream: &mut TcpStream,
) -> crate::HFSResult<()> {
    let mut buf = [0u8; 16];
    let _ = down_stream.read_exact(&mut buf).await?;
    up_stream.write_all(&buf).await?;
    Ok(())
}

async fn receive_and_send(
    mut up_orh: OwnedReadHalf,
    mut down_owh: OwnedWriteHalf,
    pkt_tx: Sender<Packet>,
) -> crate::HFSResult<()> {
    let mut buffer = BytesMut::new();

    loop {
        let mut buf = Cursor::new(buffer.chunk());
        let mut last_pkt_pos = 0;
        loop {
            match Packet::check(&mut buf) {
                Ok(_) => {
                    buf.set_position(last_pkt_pos);
                    pkt_tx.send(Packet::parse(&mut buf)).await?;
                    last_pkt_pos = buf.position();
                }
                Err(Error::InComplete) => break,
                Err(err) => return Err(err.into()),
            }
        }
        let len = last_pkt_pos as usize;
        down_owh.write_all(&buffer[..len]).await?;
        buffer.advance(len);

        if !stream_read_buf(&mut up_orh, &mut buffer).await? {
            return Ok(());
        };
    }
}

async fn receive_and_resp(up_stream: TcpStream, pkt_tx: Sender<Packet>) -> crate::HFSResult<()> {
    let mut up_stream = BufWriter::new(up_stream);
    let mut buffer = BytesMut::new();
    let crc = CRC32::new();

    loop {
        let mut buf = Cursor::new(buffer.chunk());
        let mut last_pkt_pos = 0;
        loop {
            match Packet::check(&mut buf) {
                Ok(_) => {
                    buf.set_position(last_pkt_pos);
                    let pkt = Packet::parse(&mut buf);
                    let pkt_ack = pkt.get_ack(&crc);
                    let ack_buf = pkt_ack.to_bytes();
                    up_stream.write_all(&ack_buf).await?;

                    if pkt_ack.status != Status::Success {
                        up_stream.flush().await?;
                        return Err("Packet demage".into());
                    }

                    pkt_tx.send(pkt).await?;
                    last_pkt_pos = buf.position();
                }
                Err(Error::InComplete) => break,
                Err(err) => return Err(err.into()),
            }
        }

        if last_pkt_pos != 0 {
            buffer.advance(last_pkt_pos as usize);
            up_stream.flush().await?;
        }

        if !stream_read_buf(&mut up_stream, &mut buffer).await? {
            return Ok(());
        };
    }
}

async fn transfer_ack(
    mut up_owh: OwnedWriteHalf,
    mut down_orh: OwnedReadHalf,
) -> crate::HFSResult<()> {
    loop {
        let len = io::copy(&mut down_orh, &mut up_owh).await?;
        if len == 0 {
            break;
        }
    }

    Ok(())
}

async fn to_disk(
    mut pkt_rx: Receiver<Packet>,
    mut replica_file: ReplicaFile,
) -> crate::HFSResult<()> {
    let crc = CRC32::new();

    while let Some(pkt) = pkt_rx.recv().await {
        if let Err(err) = replica_file.write_pkt(&pkt, &crc).await {
            return Err(err.into());
        }
    }

    replica_file.update_version().await?;

    Ok(())
}
