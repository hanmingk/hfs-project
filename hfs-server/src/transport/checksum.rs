use crc::{Crc, CRC_32_ISCSI};

pub struct CRC32(Crc<u32>);

impl CRC32 {
    pub fn new() -> CRC32 {
        CRC32(Crc::<u32>::new(&CRC_32_ISCSI))
    }

    pub fn checksum_size(&self) -> usize {
        std::mem::size_of::<u32>()
    }

    pub fn checksum(&self, bytes: &[u8]) -> u32 {
        self.0.checksum(bytes)
    }

    pub fn check_data(&self, bytes: &[u8], checknum: u32) -> bool {
        self.0.checksum(bytes) == checknum
    }
}
