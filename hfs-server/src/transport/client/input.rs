use std::sync::Arc;

use bytes::{Buf, BytesMut};
use hfs_proto::{
    chunk::{MasterChunkClient, ReplicaDamageRequest, Status},
    cluster::{ClusterClient, ServiceAddrRequest, ServiceType},
    master::{CloseFileRequest, FileMetaInfo, FileSystemClient, OpenFileRequest},
};
use tokio::net::TcpStream;
use volo::FastStr;

use crate::{
    chunk::cfg::DEFAULT_REPLICA_SIZE,
    grpc_client::{cluster_client, fs_client, master_chunk_client},
    transport::{checksum::CRC32, chunkserver::connect_download, packet::Packet},
};

pub struct HFSInputStream {
    fs_client: Arc<FileSystemClient>,
    c_client: Arc<ClusterClient>,
    mc_client: Arc<MasterChunkClient>,
    file_meta: FileMetaInfo,
    stream: Option<TcpStream>,
    buffer: BytesMut,
    crc: CRC32,
    position: usize,
    replica_pos: usize,
}

impl HFSInputStream {
    pub async fn open(token: FastStr, path: FastStr) -> crate::HFSResult<HFSInputStream> {
        let fs_client = Arc::new(fs_client());
        let c_client = Arc::new(cluster_client());
        let mc_client = Arc::new(master_chunk_client());

        let file_meta = match fs_client
            .open_file(OpenFileRequest {
                token,
                file_path: path,
                lease_id: 0,
            })
            .await
        {
            Ok(resp) => resp.into_inner().meta_info.unwrap(),
            Err(err) => return Err(err.message().into()),
        };

        Ok(HFSInputStream {
            fs_client,
            c_client,
            mc_client,
            file_meta,
            stream: None,
            buffer: BytesMut::new(),
            crc: CRC32::new(),
            position: 0,
            replica_pos: 0,
        })
    }

    async fn report_demage(&mut self) -> crate::HFSResult<()> {
        if self.position as u64 >= self.file_meta.len {
            return Ok(());
        }

        let chunk_index = self.position / DEFAULT_REPLICA_SIZE;

        let chunk_info = unsafe { self.file_meta.chunks.get_unchecked_mut(chunk_index) };

        if let Some(replica_info) = chunk_info.replicas.get_mut(self.replica_pos) {
            replica_info.status = Status::UnderRecover;

            let _ = self
                .mc_client
                .replica_damage(ReplicaDamageRequest {
                    chunk_id: chunk_info.chunk_id,
                    replica_id: replica_info.replica_id,
                    server_id: replica_info.server_id,
                })
                .await?;
        }

        Ok(())
    }

    async fn next_stream(&mut self) -> crate::HFSResult<Option<TcpStream>> {
        if self.position as u64 >= self.file_meta.len {
            return Ok(None);
        }

        let chunk_index = self.position / DEFAULT_REPLICA_SIZE;

        let chunk_info = unsafe { self.file_meta.chunks.get_unchecked_mut(chunk_index) };

        while let Some((index, replica_info)) = chunk_info
            .replicas
            .iter_mut()
            .enumerate()
            .find(|(_, r)| (*r).status == Status::Complete)
        {
            let addr = self
                .c_client
                .service_addr(ServiceAddrRequest {
                    server_id: replica_info.server_id,
                    service_type: ServiceType::Tcp,
                })
                .await?
                .into_inner()
                .socket_addr;

            if let Ok(stream) = connect_download(addr.as_str(), replica_info.replica_id).await {
                self.replica_pos = index;
                // println!(
                //     "Get chunk_id: {}, replica_id: {}",
                //     chunk_info.chunk_id, replica_info.replica_id
                // );

                return Ok(Some(stream));
            } else {
                replica_info.status = Status::UnderConstruction;
            }
        }

        Err("No replica of file chunk available".into())
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> crate::HFSResult<i64> {
        if self.buffer.remaining() > buf.len() {
            buf.copy_from_slice(&self.buffer.chunk()[..buf.len()]);
            self.buffer.advance(buf.len());
            self.position += buf.len();
            return Ok(buf.len() as i64);
        }

        let mut bytes_len = 0;

        buf[..self.buffer.remaining()].copy_from_slice(self.buffer.chunk());
        bytes_len += self.buffer.remaining();
        self.position += bytes_len;
        self.buffer.clear();

        let mut option_stream = self.stream.take();

        while self.buffer.is_empty() {
            match &mut option_stream {
                Some(stream) => {
                    let pkt = Packet::read_from(stream).await?;

                    if !pkt.check_pkt(&self.crc) {
                        let _ = self.report_demage().await;

                        let next_stream = self.next_stream().await?;

                        if let Some(new_steam) = next_stream {
                            *stream = new_steam;
                        }

                        let rollback_len = self.position % DEFAULT_REPLICA_SIZE;
                        self.position -= rollback_len;

                        return Ok(-(rollback_len as i64));
                    }

                    let data = pkt.data();
                    let buf_remaining = buf.len() - bytes_len;
                    if data.len() > buf_remaining {
                        buf[bytes_len..].copy_from_slice(&data[..buf_remaining]);
                        self.buffer.extend(&data[buf_remaining..]);
                        bytes_len += buf_remaining;
                    } else {
                        buf[bytes_len..bytes_len + data.len()].copy_from_slice(data);
                        bytes_len += data.len();
                    }

                    self.position += bytes_len;

                    if pkt.last_pkt() {
                        let next_stream = self.next_stream().await?;

                        if let Some(new_steam) = next_stream {
                            *stream = new_steam;
                        } else {
                            return Ok(bytes_len as i64);
                        }
                    }
                }
                None => match self.next_stream().await? {
                    Some(next_stream) => option_stream = Some(next_stream),
                    None => break,
                },
            }
        }

        self.stream = option_stream;

        Ok(bytes_len as i64)
    }

    pub async fn close(self) -> crate::HFSResult<()> {
        let _ = self
            .fs_client
            .close_file(CloseFileRequest {
                lease_id: 0,
                file_id: self.file_meta.file_id,
                chunk_ids: self.file_meta.chunks.iter().map(|c| c.chunk_id).collect(),
            })
            .await?;

        Ok(())
    }
}
