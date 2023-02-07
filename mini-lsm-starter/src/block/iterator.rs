use std::sync::Arc;

use super::Block;
use crate::global_const::U16_SIZE;
use crate::utils::FromLeBytesSlice;

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut it = Self::new(block);
        it.next();
        it
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut it = Self::new(block);
        it.seek_to_key(key);
        it
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.next();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if let Some(offset) = self.block.offsets.get(self.idx) {
            let data = &self.block.data[*offset as usize..];
            let (key_len, data) = data.split_at(U16_SIZE);
            let key_len = u16::from_le_bytes_slice(key_len) as usize;
            let (key, data) = data.split_at(key_len);
            self.key = key.into();
            let (value_len, data) = data.split_at(U16_SIZE);
            let value_len = u16::from_le_bytes_slice(value_len) as usize;
            self.value = Vec::from(&data[..value_len]);
            self.idx += 1;
        } else {
            // reached end
            self.key = Vec::new();
            self.value = Vec::new();
        }
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        self.seek_to_first();
        while self.is_valid() && self.key.as_slice() < key {
            eprintln!("{}", std::str::from_utf8(self.key.as_slice()).unwrap());
            self.next();
        }
    }
}
