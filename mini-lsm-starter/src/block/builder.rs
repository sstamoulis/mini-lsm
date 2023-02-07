use bytes::BufMut;

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    block_size: usize,
    block: Block,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            block_size,
            block: Block {
                data: Vec::new(),
                offsets: Vec::new(),
            },
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if !self.is_empty() {
            let current_block_size = self.block.estimated_size();
            // sizes of offset, key_len, value_len + actual key and value sizes
            let future_block_size = current_block_size + 3 * SIZEOF_U16 + key.len() + value.len();
            if future_block_size > self.block_size {
                return false;
            }
        }

        let offset = self.block.data.len() as u16;
        self.block.offsets.push(offset);

        self.block.data.put_u16(key.len() as u16);
        self.block.data.put(key);
        self.block.data.put_u16(value.len() as u16);
        self.block.data.put(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.block.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        assert!(!self.is_empty(), "block must not be empty");
        self.block
    }
}
