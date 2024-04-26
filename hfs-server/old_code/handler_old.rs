use std::{io::Cursor, net::SocketAddr};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};

use crate::{
    shutdown::Shutdown,
    transport::message::{Error, PipelineAck, PipelineBuildMsg},
};

use super::receiver::receiver_run;

#[derive(Debug)]
pub struct Handler {
    pub stream: TcpStream,
    pub shutdown: Shutdown,
    pub _shutdown_complete: mpsc::Sender<()>,
}

pub async fn run(mut handler: Handler) -> crate::HFSResult<()> {
    match parse_op(&mut handler.stream).await? {
        TransportOp::Upload => {
            println!("Pipeline build");
            let down_stream = upload_init(&mut handler.stream).await?;
            println!("Pipeline builded");
            receiver_run(handler.stream, down_stream).await?;
        }
        TransportOp::Download => todo!(),
    }

    Ok(())
}

pub async fn connect_with_upload(socket_addr: SocketAddr) -> Result<TcpStream, crate::Error> {
    let mut stream = TcpStream::connect(socket_addr).await?;
    stream.write_u8(TransportOp::Upload.to_byte()).await?;
    Ok(stream)
}

pub async fn connect_with_download(socket_addr: SocketAddr) -> Result<TcpStream, crate::Error> {
    let mut stream = TcpStream::connect(socket_addr).await?;
    stream.write_u8(TransportOp::Download.to_byte()).await?;
    Ok(stream)
}

pub async fn upload_init(stream: &mut TcpStream) -> crate::HFSResult<Option<TcpStream>> {
    let mut buffer = BytesMut::new();
    loop {
        let mut buf = Cursor::new(&buffer[..]);
        match PipelineBuildMsg::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let Ok(mut pbm) = PipelineBuildMsg::deserialize(&mut buf) else {
                    repo_up_ack(stream, false).await?;
                    return Err("Parse pipeline build message error".into());
                };
                buffer.advance(len);

                let Some(mut down_stream) = connect_down_pipeline(stream, &mut pbm).await? else {
                    return Ok(None);
                };
                // waiting down ack
                waiting_down_ack(stream, &mut down_stream).await?;
                return Ok(Some(down_stream));
            }
            Err(Error::InComplete) => {}
            Err(err) => return Err(err.into()),
        }

        if 0 == stream.read_buf(&mut buffer).await? {
            if buffer.is_empty() {
                return Ok(None);
            } else {
                return Err("connection reset by peer".into());
            }
        }
    }
}

async fn parse_op(socket: &mut TcpStream) -> Result<TransportOp, crate::Error> {
    let mut op = [0u8; 1];
    println!("1");
    let _ = socket.read_exact(&mut op).await?;
    println!("2");
    match op[0] {
        1 => Ok(TransportOp::Upload),
        2 => Ok(TransportOp::Download),
        _ => Err("Unknown operation".into()),
    }
}

async fn waiting_down_ack(
    up_stream: &mut TcpStream,
    down_stream: &mut TcpStream,
) -> crate::HFSResult<()> {
    let mut ack_buffer = [0u8; 5];
    let _ = down_stream.read_exact(&mut ack_buffer).await?;
    up_stream.write_all(&ack_buffer).await?;
    if ack_buffer[4] == 0 {
        return Err("Waitting down ack error".into());
    }
    Ok(())
}

async fn repo_up_ack(stream: &mut TcpStream, status: bool) -> crate::HFSResult<()> {
    let ack = PipelineAck::from_bytes([127, 0, 0, 1, status as u8]);
    stream.write_all(ack.to_bytes()).await?;
    Ok(())
}

async fn connect_down_pipeline(
    stream: &mut TcpStream,
    pbm: &mut PipelineBuildMsg,
) -> crate::HFSResult<Option<TcpStream>> {
    match pbm.next_conn_addr() {
        Some(addr) => {
            let mut down_stream = connect_with_upload(addr).await?;
            let mut pbm_bytes = Cursor::new(Vec::new());
            pbm.serialize(&mut pbm_bytes).unwrap();
            down_stream.write_all(&pbm_bytes.into_inner()).await?;
            Ok(Some(down_stream))
        }
        None => {
            // Last node of pipeline, send ack
            let ack = PipelineAck::from_bytes([127, 0, 0, 1, true as u8]);
            stream.write_all(ack.to_bytes()).await?;
            Ok(None)
        }
    }
}

#[derive(Debug)]
pub enum TransportOp {
    Upload,
    Download,
}

impl TransportOp {
    pub fn to_byte(&self) -> u8 {
        match self {
            TransportOp::Upload => 1,
            TransportOp::Download => 2,
        }
    }
}
