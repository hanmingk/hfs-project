use bytes::BytesMut;
use tokio::io::AsyncReadExt;

/// If false is returned it means the remote connection has been closed
/// Otherwise the remote connection is not closed
pub async fn stream_read_buf<Reader: AsyncReadExt + Unpin>(
    stream: &mut Reader,
    buf: &mut BytesMut,
) -> crate::HFSResult<bool> {
    if 0 == stream.read_buf(buf).await? {
        if buf.is_empty() {
            return Ok(false);
        } else {
            return Err("connection reset by peer".into());
        }
    }
    Ok(true)
}
