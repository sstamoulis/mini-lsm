mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        unimplemented!()
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
