mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        let num_of_elements = self.offsets.len();
        buf.put_u16(num_of_elements as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 * (num_of_elements + 1);
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut c| c.get_u16())
            .collect();
        let data = data[..data_end].to_vec();
        Self { data, offsets }
    }

    pub fn estimated_size(&self) -> usize {
        1 // num_of_elements size
        + self.data.len() + self.offsets.len() * SIZEOF_U16
    }
}

#[cfg(test)]
mod tests;
