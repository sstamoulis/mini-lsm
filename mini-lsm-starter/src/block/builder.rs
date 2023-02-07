use super::Block;

use crate::global_const::U16_SIZE;

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
        let current_block_size = self.block.estimated_size();
        let future_block_size = current_block_size 
            + 3 * U16_SIZE // offset + key_len + value_len size
            + key.len() + value.len();
        if future_block_size> self.block_size {
            false
        } else {
            let offset = self.block.data.len() as u16;
            self.block.offsets.push(offset);

            let key_len = key.len() as u16;
            let value_len = value.len() as u16;
            self.block.data.extend(key_len.to_le_bytes());
            self.block.data.extend_from_slice(key);
            self.block.data.extend(value_len.to_le_bytes());
            self.block.data.extend_from_slice(value);
            
            true
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.block.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        self.block
    }
}
