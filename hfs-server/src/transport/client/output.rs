use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use bytes::{Buf, BytesMut};
use hfs_proto::{
    chunk::{
        ChunkAddRequest, ChunkAddResponse, ChunkMetaInfo, MasterChunkClient, ReplicaAddRequest,
        ReplicaInfo, Status, UploadCompleteRequest,
    },
    cluster::{ClusterClient, ServiceAddrRequest, ServiceType},
    master::{CloseFileRequest, FileMetaInfo, FileSystemClient, OpenFileRequest},
};
use tokio::{
    io::AsyncReadExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
};
use volo::FastStr;

use crate::{
    chunk::cfg::DEFAULT_REPLICA_SIZE,
    grpc_client::{chunk_client, cluster_client, fs_client, master_chunk_client},
    transport::{
        cfg::{PACKET_CHECKSUM_SIZE, PACKET_CHUNKS_SIZE},
        checksum::CRC32,
        chunkserver::connect_upload,
        message::{ConnInfo, MetaInfo, PipelineBuildMsg},
        packet::{Packet, PacketAck, Status as PStatus},
    },
};

const PACKET_MAX_CHUNKS: usize = 126; // max chunk num
type AckQueue = Arc<Mutex<VecDeque<(u32, u32)>>>;

pub struct HFSOutputStream {
    fs_client: Arc<FileSystemClient>,
    c_client: Arc<ClusterClient>,
    mc_client: Arc<MasterChunkClient>,
    file_meta: FileMetaInfo,
    pkt_tx: Option<mpsc::Sender<Packet>>,
    cache_pkt: Packet,
    buffer: BytesMut,
    seq_id: u32,
    offset: usize,
    task_complete_tx: mpsc::Sender<()>,
    task_complete_rx: mpsc::Receiver<()>,
}

impl HFSOutputStream {
    pub async fn open(token: FastStr, path: FastStr) -> Result<HFSOutputStream, String> {
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

        let offset = match file_meta.chunks.last() {
            Some(c) => c.num_bytes,
            None => 0,
        };

        let (task_complete_tx, task_complete_rx) = mpsc::channel(1);

        let mut output_stream = HFSOutputStream {
            fs_client,
            c_client,
            mc_client,
            file_meta,
            pkt_tx: None,
            cache_pkt: Packet::new(0, 0, 0, 0),
            buffer: BytesMut::new(),
            seq_id: 0,
            offset: offset as usize,
            task_complete_tx,
            task_complete_rx,
        };

        let p = output_stream.next_pkt();
        output_stream.cache_pkt = p;

        Ok(output_stream)
    }

    pub async fn output_stream<R: AsyncReadExt + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> crate::HFSResult<()> {
        loop {
            while self.buffer.remaining() != 0 {
                self.write_buf().await?;
            }

            if 0 == reader.read_buf(&mut self.buffer).await? {
                break;
            }
        }

        Ok(())
    }

    async fn write_buf(&mut self) -> crate::HFSResult<()> {
        let write_len = self.cache_pkt.write_buf(self.buffer.chunk());

        if self.buffer.remaining() != 0 && write_len == 0 {
            self.send_pkt().await?;
        }

        self.offset += write_len;

        self.buffer.advance(write_len);

        Ok(())
    }

    fn next_pkt(&mut self) -> Packet {
        if self.cache_pkt.last_pkt() {
            self.update_info();
        }

        if self.offset == DEFAULT_REPLICA_SIZE {
            self.offset = 0;
        }

        self.seq_id += 1;

        if self.offset + PACKET_MAX_CHUNKS * PACKET_CHUNKS_SIZE > DEFAULT_REPLICA_SIZE {
            let data_len = DEFAULT_REPLICA_SIZE - self.offset;

            let csum_len =
                (data_len + PACKET_CHUNKS_SIZE - 1) / PACKET_CHUNKS_SIZE * PACKET_CHECKSUM_SIZE;

            let mut pkt = Packet::new(
                self.seq_id,
                self.offset as u64,
                csum_len,
                csum_len + data_len,
            );
            pkt.set_last_pkt(true);
            pkt
        } else {
            let csum_len = PACKET_MAX_CHUNKS * PACKET_CHECKSUM_SIZE;
            let pkt_size = PACKET_MAX_CHUNKS * (PACKET_CHECKSUM_SIZE + PACKET_CHUNKS_SIZE);

            Packet::new(self.seq_id, self.offset as u64, csum_len, pkt_size)
        }
    }

    fn update_info(&mut self) {
        if let Some(info) = self.file_meta.chunks.last_mut() {
            info.num_bytes = self.offset as u32;
        }
    }

    async fn send_pkt(&mut self) -> Result<(), String> {
        let is_last = self.cache_pkt.last_pkt();

        let new_pkt = self.next_pkt();
        let pkt_sent = std::mem::replace(&mut self.cache_pkt, new_pkt);

        match &self.pkt_tx {
            Some(pkt_tx) => {
                if let Err(_) = pkt_tx.send(pkt_sent).await {
                    return Err("Send task has close".into());
                }
            }
            None => {
                let target_chunk = self.next_chunk().await?;

                let (orh, owh) = build_pipeline(&self.c_client, &target_chunk)
                    .await?
                    .into_split();

                let (stop_tx, stop_rx) = oneshot::channel();

                let ack_queue = Arc::new(Mutex::new(VecDeque::new()));

                tokio::spawn(ack_task(
                    self.mc_client.clone(),
                    ack_queue.clone(),
                    target_chunk,
                    orh,
                    stop_tx,
                    self.task_complete_tx.clone(),
                ));

                let (pkt_tx, pkt_rx) = mpsc::channel(1024 * 64);
                tokio::spawn(send_task(
                    ack_queue,
                    pkt_rx,
                    owh,
                    stop_rx,
                    self.task_complete_tx.clone(),
                ));

                if let Err(_) = pkt_tx.send(pkt_sent).await {
                    return Err("Send task has close".into());
                }

                self.pkt_tx = Some(pkt_tx);
            }
        }

        if is_last {
            self.pkt_tx = None;
        }

        Ok(())
    }

    async fn next_chunk(&mut self) -> Result<ChunkMetaInfo, String> {
        let add_new = match self.file_meta.chunks.last() {
            Some(info) => info.num_bytes == info.chunk_size,
            None => true,
        };

        if add_new {
            if let Err(err) = add_chunk(&self.c_client, &self.mc_client, &mut self.file_meta).await
            {
                return Err(err.message().to_string());
            }
        }

        self.file_meta
            .chunks
            .last()
            .map(|c| c.clone())
            .ok_or("file chunk not enough".to_string())
    }

    pub async fn close(mut self) -> Result<(), String> {
        let mut err_msg = String::new();

        if self.cache_pkt.data_len() != 0 {
            self.cache_pkt.set_last_pkt(true);
            if let Err(err) = self.send_pkt().await {
                err_msg = err;
            }
        }

        let HFSOutputStream {
            fs_client,
            file_meta,
            pkt_tx,
            task_complete_tx,
            mut task_complete_rx,
            ..
        } = self;

        drop(pkt_tx);
        drop(task_complete_tx);

        let _ = task_complete_rx.recv().await;

        let chunk_ids = file_meta.chunks.iter().map(|c| c.chunk_id).collect();

        let _ = fs_client
            .close_file(CloseFileRequest {
                lease_id: 0,
                file_id: file_meta.file_id,
                chunk_ids,
            })
            .await
            .map_err(|err| err.message().to_string())?;

        if !err_msg.is_empty() {
            return Err(err_msg);
        }

        Ok(())
    }
}

async fn send_task(
    ack_queue: AckQueue,
    mut pkt_rx: mpsc::Receiver<Packet>,
    mut owh: OwnedWriteHalf,
    close_rx: oneshot::Receiver<()>,
    _complete_tx: mpsc::Sender<()>,
) -> crate::HFSResult<()> {
    let crc = CRC32::new();

    tokio::select! {
        _ = async {
            while let Some(mut pkt) = pkt_rx.recv().await {
                pkt.gen_checksum(&crc);

                {
                    let mut queue = ack_queue.lock().unwrap();
                    queue.push_back((pkt.seq_id(), pkt.data_len() as u32));
                }

                if let Err(_) = pkt.write_to(&mut owh).await {
                    return;
                }
            }
        } => {},
        _ = close_rx => {},
    }

    Ok(())
}

async fn ack_task(
    mc_client: Arc<MasterChunkClient>,
    ack_queue: AckQueue,
    chunk_info: ChunkMetaInfo,
    mut orh: OwnedReadHalf,
    close_tx: oneshot::Sender<()>,
    _complete_tx: mpsc::Sender<()>,
) -> Result<(), String> {
    let mut is_demage = false;
    let mut num_bytes = chunk_info.num_bytes;

    loop {
        match PacketAck::deserialize(&mut orh).await {
            Ok(pkt_ack) => {
                let mut queue = ack_queue.lock().unwrap();

                if let Some((seq_id, len)) = queue.pop_front() {
                    if seq_id != pkt_ack.seq_id || pkt_ack.status != PStatus::Success {
                        is_demage = true;
                        break;
                    }

                    num_bytes += len;
                }

                if pkt_ack.is_last {
                    break;
                }
            }
            Err(_) => {
                is_demage = true;
                break;
            }
        }
    }

    if is_demage {
        let _ = mc_client
            .upload_complete(UploadCompleteRequest {
                chunk_id: chunk_info.chunk_id,
                num_bytes: chunk_info.num_bytes,
                size: DEFAULT_REPLICA_SIZE as u32,
                status: false,
            })
            .await;

        let _ = close_tx.send(());

        return Err("Packet demage".into());
    }

    let _ = mc_client
        .upload_complete(UploadCompleteRequest {
            chunk_id: chunk_info.chunk_id,
            num_bytes,
            size: chunk_info.chunk_size,
            status: true,
        })
        .await;

    let _ = close_tx.send(());

    Ok(())
}

async fn build_pipeline(
    c_client: &ClusterClient,
    meta_info: &ChunkMetaInfo,
) -> Result<TcpStream, String> {
    let mut conns = Vec::new();
    for info in meta_info.replicas.iter() {
        let addr: SocketAddr = match c_client
            .service_addr(ServiceAddrRequest {
                server_id: info.server_id,
                service_type: ServiceType::Tcp,
            })
            .await
        {
            Ok(resp) => resp.into_inner().socket_addr.parse().unwrap(),
            Err(err) => return Err(err.message().to_string()),
        };

        if let SocketAddr::V4(ipv4) = addr {
            conns.push(ConnInfo {
                ipv4: ipv4.ip().octets(),
                port: ipv4.port(),
                meta_info: MetaInfo {
                    csum_len: 0,
                    chunks_len: 0,
                    replica_id: info.replica_id,
                },
            });
        }
    }

    if conns.is_empty() {
        return Err("Number of pipeline node is zero".into());
    }

    let pbm = PipelineBuildMsg::new(conns);
    connect_upload(pbm).await.map_err(|err| err.to_string())
}

async fn add_chunk(
    c_client: &ClusterClient,
    mc_client: &MasterChunkClient,
    meta_info: &mut FileMetaInfo,
) -> Result<(), volo_grpc::Status> {
    let ChunkAddResponse {
        chunk_id,
        server_id: server_ids,
    } = mc_client.chunk_add(ChunkAddRequest {}).await?.into_inner();

    let mut replicas = Vec::new();
    for server_id in server_ids {
        let addr: SocketAddr = c_client
            .service_addr(ServiceAddrRequest {
                server_id,
                service_type: ServiceType::Rpc,
            })
            .await?
            .into_inner()
            .socket_addr
            .parse()
            .unwrap();

        let replica_id = chunk_client(addr)
            .replica_add(ReplicaAddRequest { chunk_id })
            .await?
            .into_inner()
            .replica_id;

        replicas.push(ReplicaInfo {
            server_id,
            replica_id,
            status: Status::Complete,
        })
    }

    meta_info.chunks.push(ChunkMetaInfo {
        chunk_id,
        chunk_size: DEFAULT_REPLICA_SIZE as u32,
        num_bytes: 0,
        version: 0,
        status: Status::Complete,
        replicas,
    });

    Ok(())
}
