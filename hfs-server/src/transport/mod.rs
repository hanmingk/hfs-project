pub mod checksum;
pub mod chunkserver;
pub mod client;
pub mod message;
pub mod meta_file;
pub mod packet;
pub mod replica_file;
pub mod utils;

// pub const PRE_PACKET_SIZE: usize = 64 * 1024;
// pub const DEFAULT_CHECKNUM_SIZE: usize = 4;
// pub const DEFAULT_CHUNK_SIZE: usize = 512;
// pub const DEFAULT_FRAME_SIZE: usize = DEFAULT_CHUNK_SIZE + DEFAULT_CHECKNUM_SIZE;

pub mod cfg {
    pub const PACKET_CHUNKS_SIZE: usize = 512;
    pub const PACKET_CHECKSUM_SIZE: usize = 4; // crc32
    pub const DEFAULT_PACKET_SIZE: usize = 64 * 1024; // 64kb
}
// #[cfg(test)]
// mod test {
//     use std::{io::Cursor, net::SocketAddr};

//     use bytes::{Buf, BytesMut};
//     use tokio::{
//         io::{AsyncReadExt, AsyncWriteExt},
//         net::{TcpListener, TcpStream},
//     };

//     use super::message::{Error, PipelineBuildMsg};

//     async fn process(socket: TcpStream, addr: SocketAddr) {
//         let mut buf = BytesMut::new();
//         let (mut up_orh, mut up_owh) = socket.into_split();
//         let mut down_stream = None;
//         loop {
//             let mut cor_buf = Cursor::new(&buf[..]);
//             match PipelineBuildMsg::check(&mut cor_buf) {
//                 Ok(_) => {
//                     let len = cor_buf.position() as usize;
//                     cor_buf.set_position(0);
//                     let mut pbm = PipelineBuildMsg::deserialize(&mut cor_buf).unwrap();
//                     buf.advance(len);
//                     match pbm.next_conn() {
//                         Some(conn) => {
//                             let stream = TcpStream::connect(conn.to_socket_addr()).await.unwrap();
//                             let (down_orh, mut down_owh) = stream.into_split();
//                             let mut pbm_bytes = Cursor::new(Vec::new());
//                             pbm.serialize(&mut pbm_bytes).unwrap();
//                             down_owh.write_all(&pbm_bytes.into_inner()).await.unwrap();
//                             down_stream = Some((down_orh, down_owh));
//                             break;
//                         }
//                         None => {
//                             println!("Last node pipeline send message: hello pipeline");
//                             up_owh.write_all(b"hello pipeline").await.unwrap();
//                         }
//                     }
//                 }
//                 Err(Error::InComplete) => {}
//                 _ => {
//                     println!("{:?} error", addr);
//                     return;
//                 }
//             }

//             if 0 == up_orh.read_buf(&mut buf).await.unwrap() {
//                 if buf.is_empty() {
//                     break;
//                 } else {
//                     println!("{:?} error", addr);
//                     return;
//                 }
//             }
//         }

//         if let Some((mut down_orh, _)) = down_stream {
//             let mut down_buf = BytesMut::new();
//             loop {
//                 if down_buf.len() == 14 {
//                     let msg = String::from_utf8(down_buf.to_vec()).unwrap();
//                     println!("{}", msg);
//                     up_owh.write_all(msg.as_bytes()).await.unwrap();
//                     break;
//                 }

//                 if 0 == down_orh.read_buf(&mut down_buf).await.unwrap() {
//                     if down_buf.is_empty() {
//                         break;
//                     } else {
//                         println!("{:?} error", addr);
//                         return;
//                     }
//                 }
//             }
//         }
//     }

//     #[tokio::test]
//     async fn server1() -> std::io::Result<()> {
//         let listener = TcpListener::bind("127.0.0.1:3080").await?;

//         println!("waiting connect!!");

//         let (socket, addr) = listener.accept().await?;
//         println!("{:?}: connect", addr);
//         process(socket, addr).await;

//         Ok(())
//     }

//     #[tokio::test]
//     async fn server2() -> std::io::Result<()> {
//         let listener = TcpListener::bind("127.0.0.1:4080").await?;

//         println!("waiting connect!!");

//         let (socket, addr) = listener.accept().await?;
//         println!("{:?}: connect", addr);
//         process(socket, addr).await;

//         Ok(())
//     }

//     #[tokio::test]
//     async fn server3() -> std::io::Result<()> {
//         let listener = TcpListener::bind("127.0.0.1:5080").await?;

//         println!("waiting connect!!");

//         let (socket, addr) = listener.accept().await?;
//         println!("{:?}: connect", addr);
//         process(socket, addr).await;

//         Ok(())
//     }

//     #[tokio::test]
//     async fn client() -> std::io::Result<()> {
//         let mut pbm = PipelineBuildMsg::new_test();
//         let conn = pbm.next_conn().unwrap();
//         let stream = TcpStream::connect(conn.to_socket_addr()).await.unwrap();

//         let (mut down_orh, mut down_owh) = stream.into_split();

//         let mut pbm_bytes = Cursor::new(Vec::new());
//         pbm.serialize(&mut pbm_bytes).unwrap();
//         down_owh.write_all(&pbm_bytes.into_inner()).await.unwrap();

//         let mut down_buf = BytesMut::new();
//         loop {
//             if down_buf.len() == 14 {
//                 let msg = String::from_utf8(down_buf.to_vec()).unwrap();
//                 println!("{}", msg);
//                 break;
//             }

//             if 0 == down_orh.read_buf(&mut down_buf).await.unwrap() {
//                 if down_buf.is_empty() {
//                     break;
//                 } else {
//                     println!("client error");
//                     return Ok(());
//                 }
//             }
//         }

//         Ok(())
//     }
// }
