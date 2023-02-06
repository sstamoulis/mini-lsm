mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.size());
        bytes.extend_from_slice(&self.data);
        bytes.extend(self.offsets.iter().flat_map(|o| o.to_le_bytes()));
        let num_of_elements = self.offsets.len() as u16;
        bytes.extend(num_of_elements.to_le_bytes());
        bytes.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        unimplemented!()
    }

    fn size(&self) -> usize {
        1 // num_of_elements size
        + self.data.len() + self.offsets.len() * U16_SIZE
    }
}

#[cfg(test)]
mod tests;
