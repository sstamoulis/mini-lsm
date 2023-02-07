use super::Block;
use bytes::Buf;
use std::sync::Arc;

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
        it.seek_to_first();
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
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        self.seek_to_first();
        while self.is_valid() && self.key.as_slice() < key {
            eprintln!("{}", std::str::from_utf8(self.key.as_slice()).unwrap());
            self.next();
        }
    }

    /// Seeks to idx-th block
    fn seek_to(&mut self, idx: usize) {
        self.idx = idx;
        if let Some(offset) = self.block.offsets.get(idx) {
            self.seek_to_offset(*offset as usize);
        } else {
            self.key.clear();
            self.value.clear();
        }
    }

    /// Seeks to the byte offset
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        let key_len = entry.get_u16() as usize;
        self.key.clear();
        self.key.extend(entry.copy_to_bytes(key_len));
        let value_len = entry.get_u16() as usize;
        self.value.clear();
        self.value.extend(entry.copy_to_bytes(value_len));
    }
}
