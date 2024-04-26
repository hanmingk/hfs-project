use std::net::SocketAddr;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    chunk::chunkserver::{
        manager::{update_complete, ReplicaManager},
        replica::{history_from, history_to},
    },
    transport::{checksum::CRC32, packet::Packet, replica_file::ReplicaFile},
};

use super::{report_demage, service::TransportOp};

const COPY_ACK: [u8; 18] = *b"CopiedReplicaIdAck";
const REPLICA_ID_PREFIX: [u8; 10] = *b"Replica_Id";

pub async fn connect_copy(addr: SocketAddr, replica_id: i64) -> crate::HFSResult<TcpStream> {
    let mut header_buf = [0u8; 30];

    let opcode = TransportOp::Copy;
    header_buf[0..12].copy_from_slice(&opcode.to_bytes());
    header_buf[12..22].copy_from_slice(&REPLICA_ID_PREFIX);
    header_buf[22..30].copy_from_slice(&replica_id.to_be_bytes());

    let mut socket = TcpStream::connect(addr).await?;
    socket.write_all(&header_buf).await?;
    socket.flush().await?;

    let mut ack_buf = [0u8; 18];
    socket.read_exact(&mut ack_buf).await?;

    if ack_buf != COPY_ACK {
        return Err("Replica copy connect error".into());
    }

    Ok(socket)
}

pub async fn copy_task(
    mut stream: TcpStream,
    replica_mgr: &ReplicaManager,
) -> crate::HFSResult<()> {
    let mut header_buf = [0u8; 18];

    stream.read_exact(&mut header_buf).await?;

    if &header_buf[0..10] != &REPLICA_ID_PREFIX {
        return Err("Replica id prefix error".into());
    }

    let replica_id = i64::from_be_bytes(header_buf[10..18].try_into().unwrap());

    let meta_info = replica_mgr
        .get_replica_meta(&replica_id)
        .ok_or("Unknown replica id")?;

    let mut replica_file = ReplicaFile::from(&replica_mgr.base_dir, meta_info).await?;

    let chunk_id = replica_file.chunk_id();

    let crc = CRC32::new();

    // send copy ack and then send packet
    stream.write_all(&COPY_ACK).await?;

    let mut packets = replica_file.packets().await;
    while let Some(mut packet) = packets.next().await {
        if !packet.check_pkt(&crc) {
            report_demage(replica_mgr, chunk_id, replica_id).await;

            return Err("Replica file demage".into());
        }

        packet.write_to(&mut stream).await?;
    }

    let history_info = replica_file.history_info().await?;
    history_to(&mut stream, &history_info).await?;

    Ok(())
}

pub async fn receive_copy(
    mut stream: TcpStream,
    replica_id: i64,
    replica_mgr: &ReplicaManager,
) -> crate::HFSResult<()> {
    let meta_info = replica_mgr
        .get_update_info(&replica_id)
        .ok_or("Unknown update replica id")?;

    let mut replica_file = ReplicaFile::from(&replica_mgr.base_dir, meta_info).await?;
    replica_file.reset().await?;

    let crc = CRC32::new();

    loop {
        let pkt = Packet::read_from(&mut stream).await?;

        replica_file.write_pkt(&pkt, &crc).await?;

        if pkt.last_pkt() {
            break;
        }
    }

    // receive history version info
    let mut history_info = history_from(&mut stream).await?;

    for item in history_info.iter_mut() {
        item.replica_id = replica_id;
    }

    replica_file.reset_history(history_info).await?;

    drop(replica_file);

    update_complete(replica_id, replica_mgr, false).await;

    Ok(())
}
