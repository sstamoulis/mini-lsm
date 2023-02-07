mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut, Buf};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let estimated_size = self.estimated_size();
        let mut bytes = BytesMut::with_capacity(estimated_size);
        bytes.put_slice(&self.data);
        assert_ne!(0, self.data.len());
        for offset in self.offsets {
            bytes.put_u16(offset);
        }
        let num_of_elements = self.offsets.len() as u16;
        bytes.put_u16(num_of_elements);

        assert_eq!(estimated_size, bytes.len());

        bytes.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        use std::io::Cursor
        let num_of_elements = data.po;
        let (data, offsets) = data.split_at(data.len() - (num_of_elements as usize + 1) * U16_SIZE);

        let offsets: Vec<u16> = offsets
            .chunks_exact(U16_SIZE)
            .take(num_of_elements as usize)
            .map(|b| u16::from_le_bytes_slice(b))
            .collect();
        let data: Vec<u8> = data.to_vec();
        Block { data, offsets }
    }

    pub fn estimated_size(&self) -> usize {
        1 // num_of_elements size
        + self.data.len() + self.offsets.len() * U16_SIZE
    }
}

#[cfg(test)]
mod tests;
