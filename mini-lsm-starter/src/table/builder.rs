use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{Block, BlockBuilder, BlockIterator},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    blocks: Vec<Block>,
    block_builder: BlockBuilder,
    block_size: usize,
    offset: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: Vec::new(),
            blocks: Vec::new(),
            block_builder: BlockBuilder::new(block_size),
            block_size,
            offset: 0,
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if !self.block_builder.add(key, value) {
            // BlockBuilder is full, build and store block
            let block_builder =
                std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
            let block = Arc::new(block_builder.build());
            {
                let block_iterator = BlockIterator::create_and_seek_to_first(block.clone());
                let first_key = Bytes::copy_from_slice(block_iterator.key());
                self.meta.push(BlockMeta {
                    offset: self.offset,
                    first_key,
                });
            }
            if let Ok(block) = Arc::try_unwrap(block) {
                self.blocks.push(block);
            } else {
                panic!("Could not unwrap block Arc");
            }

            // Add key-value pair in new BlockBuilder
            if !self.block_builder.add(key, value) {
                panic!("Cannot fit key-value pair");
            }
        }
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.blocks.iter().map(|b| b.estimated_size()).sum()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        _id: usize,
        _block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut data = BytesMut::new();
        for block in self.blocks {
            data.put(block.encode());
        }
        {
            let mut buf = Vec::new();
            BlockMeta::encode_block_meta(&self.meta, &mut buf);
            data.put(Bytes::from(buf));
        }
        data.put_u32(self.offset as u32);
        let data = data.freeze();
        let file = FileObject::create(path.as_ref(), Vec::from(data))?;
        Ok(SsTable {
            file,
            block_metas: self.meta,
            block_meta_offset: self.offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
