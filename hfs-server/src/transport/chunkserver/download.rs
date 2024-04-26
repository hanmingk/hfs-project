use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
};

use crate::{chunk::chunkserver::manager::ReplicaManager, transport::replica_file::ReplicaFile};

use super::{report_demage, service::TransportOp};

const DOWNLOAD_ACK: &[u8; 11] = b"DownloadAck";

pub async fn connect_download<A: ToSocketAddrs>(
    addr: A,
    replica_id: i64,
) -> crate::HFSResult<TcpStream> {
    let mut socket = TcpStream::connect(addr).await?;
    let opcode = TransportOp::Download;
    socket.write_all(&opcode.to_bytes()).await?;
    socket.write_i64(replica_id).await?;

    let mut ack_buf = [0u8; 11];

    socket.read_exact(&mut ack_buf).await?;

    if &ack_buf != DOWNLOAD_ACK {
        return Err("Unknown download ack".into());
    }

    Ok(socket)
}

pub async fn download_task(
    mut stream: TcpStream,
    replica_mgr: &ReplicaManager,
) -> crate::HFSResult<()> {
    let mut id_buf = [0u8; 8];

    stream.read_exact(&mut id_buf).await?;

    let replica_id = i64::from_be_bytes(id_buf);

    let replica_info = replica_mgr
        .get_replica_meta(&replica_id)
        .ok_or(format!("Unknown replica id {}", replica_id))?;

    let chunk_id = replica_info.chunk_id;

    let mut replica_file = match ReplicaFile::from(&replica_mgr.base_dir, replica_info).await {
        Ok(file) => file,
        Err(err) => {
            report_demage(replica_mgr, chunk_id, replica_id).await;
            return Err(err.into());
        }
    };

    stream.write_all(DOWNLOAD_ACK).await?;

    let mut packets = replica_file.packets().await;
    while let Some(mut packet) = packets.next().await {
        packet.write_to(&mut stream).await?;
    }

    Ok(())
}
