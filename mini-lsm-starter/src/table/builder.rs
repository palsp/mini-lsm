// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice, KeyVec},
    lsm_storage::BlockCache,
    table::{FileObject, bloom::Bloom},
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            block_size,
            first_key: Vec::new(),
            last_key: Vec::new(),
            key_hashes: Vec::new(),
            data: Vec::new(),
            meta: vec![BlockMeta {
                offset: 0,
                first_key: KeyBytes::from_bytes(Bytes::new()),
                last_key: KeyBytes::from_bytes(Bytes::new()),
            }],
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let key_bytes = key.to_key_vec().into_key_bytes();
        if !self.builder.add(key, value) {
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            self.data.append(&mut builder.build().encode().to_vec());

            let offset = self.data.len();
            let _ = self.builder.add(key, value);
            self.meta.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes(Bytes::new()),
                last_key: KeyBytes::from_bytes(Bytes::new()),
            });
        }

        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
        // update current block meta
        let meta_len = self.meta.len();
        if self.meta[meta_len - 1].first_key.is_empty() {
            self.meta[meta_len - 1].first_key = key_bytes.clone();
        }
        self.meta[meta_len - 1].last_key = key_bytes.clone();

        // update sst meta
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }
        self.last_key = key.to_key_vec().into_inner();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.block_size * self.meta.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // append from on-going builder
        self.data
            .append(&mut self.builder.build().encode().to_vec());

        // append metadata
        let block_meta_offset = self.data.len() as u32;
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data
            .extend_from_slice(&block_meta_offset.to_be_bytes());

        // append bloom filter
        let bloom_offset = self.data.len() as u32;
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        bloom.encode(&mut self.data);
        self.data.extend_from_slice(&bloom_offset.to_be_bytes());

        let file = FileObject::create(path.as_ref(), self.data)?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key: KeyVec::from_vec(self.first_key).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key).into_key_bytes(),
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
