use std::io::Cursor;

use bytes::Buf;
use tokio::io::AsyncReadExt;

use super::checknum::DataCheck;

const PACKET_PREFIX: [u8; 3] = *b"\n$\r";

#[derive(Debug)]
pub struct Packet {
    seq_id: u32,
    offset: u64, // offset in chunk
    checknum_start: usize,
    checknum_pos: usize,
    data_start: usize,
    data_pos: usize,
    is_last: bool,
    buffer: Vec<u8>,
}

pub struct LoaderMut<'a> {
    checknum_size: usize,
    chunk_size: usize,
    checknum_inner: &'a mut [u8],
    data_inner: &'a mut [u8],
}

impl<'a> Iterator for LoaderMut<'a> {
    type Item = (&'a mut [u8], &'a mut [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_inner.len() == 0 {
            return None;
        }

        let cn_tmp = std::mem::take(&mut self.checknum_inner);
        let data_tmp = std::mem::take(&mut self.data_inner);

        let (checknum_buf, remaining) = cn_tmp.split_at_mut(self.checknum_size);
        self.checknum_inner = remaining;

        let (chunk_buf, remaining) = data_tmp.split_at_mut(self.chunk_size);
        self.data_inner = remaining;

        Some((checknum_buf, chunk_buf))
    }
}

pub struct Loader<'a> {
    checknum_size: usize,
    chunk_size: usize,
    checknum_inner: &'a [u8],
    data_inner: &'a [u8],
}

impl<'a> Iterator for Loader<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_inner.len() < self.chunk_size {
            return None;
        }

        let (checknum_buf, remaining) = self.checknum_inner.split_at(self.checknum_size);
        self.checknum_inner = remaining;

        let (chunk_buf, remaining) = self.data_inner.split_at(self.chunk_size);
        self.data_inner = remaining;

        Some((checknum_buf, chunk_buf))
    }
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl Packet {
    pub fn new(
        seq_id: u32,
        offset: u64,
        data_start: usize,
        buf_size: usize,
        is_last: bool,
    ) -> Packet {
        Packet {
            seq_id,
            offset,
            checknum_start: 0,
            checknum_pos: 0,
            data_start,
            data_pos: data_start,
            buffer: vec![0; buf_size],
            is_last,
        }
    }

    pub fn loader(&self, checknum_size: usize, chunk_size: usize) -> Loader {
        Loader {
            checknum_size,
            chunk_size,
            checknum_inner: &self.buffer[self.checknum_start..self.checknum_pos],
            data_inner: &self.buffer[self.data_start..self.data_pos],
        }
    }

    pub fn loader_mut(&mut self, checknum_size: usize, chunk_size: usize) -> LoaderMut {
        let (checknum_inner, data_inner) = self.buffer.split_at_mut(self.data_pos);
        LoaderMut {
            checknum_size,
            chunk_size,
            checknum_inner,
            data_inner,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data_pos == self.data_start
    }

    pub fn is_full(&self) -> bool {
        self.data_pos == self.buffer.len()
    }

    pub async fn load_data<R, V>(&mut self, reader: &mut R, validator: &V) -> tokio::io::Result<()>
    where
        R: AsyncReadExt + Unpin,
        V: DataCheck,
    {
        let checknum_size = validator.checknum_size();
        let chunk_size = validator.chunk_size();
        let mut load_data_num = 0;
        for (checknum_buf, chunk_buf) in self.loader_mut(checknum_size, chunk_size) {
            let n = reader.read(chunk_buf).await?;

            if n == 0 {
                // `0` indicates "end of stream".
                return Ok(());
            }

            load_data_num += n;
            validator.gen_checknum(&chunk_buf, checknum_buf);
        }

        if load_data_num % chunk_size == 0 {
            self.checknum_pos += load_data_num / chunk_size * checknum_size;
        } else {
            self.checknum_pos += (load_data_num / chunk_size + 1) * checknum_size;
        }
        self.data_pos += load_data_num;
        Ok(())
    }

    pub fn header_size() -> usize {
        // seq_id, offset, checknum_start, checknum_pos, data_start, data_pos, is_last, prefix
        4 + 8 + 4 * std::mem::size_of::<usize>() + 1 + 3
    }

    pub fn data_ref(&self) -> &[u8] {
        &self.buffer[self.data_start..self.data_pos]
    }

    pub fn checknum_ref(&self) -> &[u8] {
        &self.buffer[self.checknum_start..self.checknum_pos]
    }

    pub fn check_data<V: DataCheck>(&self, validator: &V) -> bool {
        for (checknum_ref, data_ref) in
            self.loader(validator.checknum_size(), validator.chunk_size())
        {
            if !validator.check_data(data_ref, checknum_ref) {
                return false;
            }
        }

        true
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::transport::checknum::CRC32;

    use super::Packet;
    use bytes::{Buf, BytesMut};
    use crc::{Crc, CRC_32_ISCSI};
    use tokio::{
        fs::File,
        io::{AsyncReadExt, AsyncWriteExt},
    };

    #[test]
    fn test_bytes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut file = File::open("/home/hmk/code/rust/hfs-project/test_data/test.txt")
                .await
                .unwrap();

            let mut buf = BytesMut::new();
            let n = file.read_buf(&mut buf).await.unwrap();
            println!("{n}");
            println!("{:?}", buf);

            let n = file.read_buf(&mut buf).await.unwrap();
            println!("{n}");
            println!("{:?}", buf);
        });
    }

    #[test]
    fn test_packet() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut packet = Packet::new(0, 0, 32, 1024, false);

            let mut file = File::open("/home/hmk/code/rust/hfs-project/test_data/test.txt")
                .await
                .unwrap();

            let crc32 = CRC32::new(124);
            packet.load_data(&mut file, &crc32).await.unwrap();
            println!("{:?}", packet.data_ref().len());

            assert_eq!(packet.check_data(&crc32), true);

            let mut file = File::create("/home/hmk/code/rust/hfs-project/test_data/test_bat.txt")
                .await
                .unwrap();

            file.write_all(packet.data_ref()).await.unwrap();
            // let mut buffer = [0; 10];
        });
    }

    #[test]
    fn test() {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);

        let buf1 = [23, 37, 34, 84, 93];
        let buf2 = [23, 37, 34, 84, 93, 0, 0, 0, 0, 0];

        assert_eq!(crc.checksum(&buf1[..]), crc.checksum(&buf2[..]));
    }

    #[test]
    fn test_mem() {
        let mut s: [u8; 11] = *b"Hello World";
        println!("{:p}", &s);
        let mut b = &mut s[..];

        let c = std::mem::take(&mut b);

        println!("{:p}", c);
        println!("b: {:?}, c: {:?}", b, c);
    }
}
