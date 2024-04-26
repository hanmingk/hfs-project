use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::mpsc::{self, Receiver},
};

use crate::transport::{
    checksum::CRC32,
    packet::{Packet, PacketAck, Status},
};

pub async fn receiver_run(
    up_stream: TcpStream,
    down_stream: Option<TcpStream>,
) -> crate::HFSResult<()> {
    match down_stream {
        Some(down_stream) => {
            receive_pkt(up_stream, down_stream).await?;
        }
        None => {
            last_receive_pkt(up_stream, CRC32::new()).await?;
        }
    }

    Ok(())
}

async fn receive_pkt(up_stream: TcpStream, down_stream: TcpStream) -> crate::HFSResult<()> {
    let (mut up_orh, up_owh) = up_stream.into_split();
    let (down_orh, mut down_owh) = down_stream.into_split();

    let (pkt_tx, pkt_rx) = mpsc::channel(10);
    tokio::spawn(packet_to_disk(pkt_rx));

    tokio::spawn(receive_ack(up_owh, down_orh));

    let mut buffer = BytesMut::new();
    loop {
        let mut cur_buf = Cursor::new(&buffer[..]);
        if let Some(pkt) = parse_packet(&mut cur_buf).await? {
            let data_len = cur_buf.position() as usize;
            pkt_tx.send(pkt).await?;
            down_owh.write_all(&buffer[..data_len]).await?;
            buffer.advance(data_len);
        }

        if 0 == up_orh.read_buf(&mut buffer).await? {
            if buffer.is_empty() {
                drop(pkt_tx);
                return Ok(());
            } else {
                drop(pkt_tx);
                return Err("connection reset by peer".into());
            }
        }
    }
}

async fn last_receive_pkt(mut up_stream: TcpStream, crc: CRC32) -> crate::HFSResult<()> {
    let (pkt_tx, pkt_rx) = mpsc::channel(10);
    tokio::spawn(packet_to_disk(pkt_rx));

    let mut buffer = BytesMut::new();
    loop {
        let mut cur_buf = Cursor::new(&buffer[..]);
        if let Some(pkt) = parse_packet(&mut cur_buf).await? {
            let data_len = cur_buf.position() as usize;
            let ack = pkt.get_ack(&crc, 512);
            if let Status::Success = ack.status {
                pkt_tx.send(pkt).await?;
            }
            up_stream.write_all(&ack.to_bytes()).await?;
            buffer.advance(data_len);
        }

        if 0 == up_stream.read_buf(&mut buffer).await? {
            if buffer.is_empty() {
                drop(pkt_tx);
                return Ok(());
            } else {
                drop(pkt_tx);
                return Err("connection reset by peer".into());
            }
        }
    }
}

async fn receive_ack(
    mut up_owh: OwnedWriteHalf,
    mut down_orh: OwnedReadHalf,
) -> crate::HFSResult<()> {
    let mut buffer = BytesMut::new();
    loop {
        let mut cur_buf = Cursor::new(&buffer[..]);
        if let Some(ack) = parse_ack(&mut cur_buf).await? {
            // TODO handle ack
            let data_len = cur_buf.position() as usize;
            up_owh.write_all(&buffer[..data_len]).await?;
            buffer.advance(data_len);
        }

        if 0 == down_orh.read_buf(&mut buffer).await? {
            if buffer.is_empty() {
                return Ok(());
            } else {
                return Err("connection reset by peer".into());
            }
        }
    }
}

async fn parse_packet(buf: &mut Cursor<&[u8]>) -> crate::HFSResult<Option<Packet>> {
    use crate::transport::packet::Error::InComplete;

    match Packet::check(buf) {
        Ok(_) => {
            buf.set_position(0);
            let pkt = Packet::parse(buf);
            Ok(Some(pkt))
        }
        Err(InComplete) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn parse_ack(buf: &mut Cursor<&[u8]>) -> crate::HFSResult<Option<PacketAck>> {
    use crate::transport::packet::Error::InComplete;

    match PacketAck::check(buf) {
        Ok(_) => {
            buf.set_position(0);
            let pkt = PacketAck::parse(buf)?;
            Ok(Some(pkt))
        }
        Err(InComplete) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn packet_to_disk(mut pkt_rx: Receiver<Packet>) -> crate::HFSResult<()> {
    let process_id = std::process::id();
    let paht = format!(
        "/home/hmk/code/rust/hfs-project/test_data/test{}.txt",
        process_id
    );
    let mut file = File::options()
        .create(true)
        .append(true)
        .open(&paht)
        .await?;
    while let Some(pkt) = pkt_rx.recv().await {
        file.write_all(pkt.get_data()).await?;
        // TODO to disk
    }
    Ok(())
}
