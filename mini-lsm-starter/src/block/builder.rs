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

use bytes::{BufMut, BytesMut};

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn compute_overlap(&self, key: KeySlice) -> u16 {
        let mut overlap = 0;

        for i in 0..key.len() {
            if i >= self.first_key.len() {
                break;
            }

            if key.raw_ref()[i] != self.first_key.raw_ref()[i] {
                break;
            }

            overlap += 1;
        }

        overlap
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let Ok(key_len) = u16::try_from(key.len()) else {
            return false;
        };
        let Ok(value_len) = u16::try_from(value.len()) else {
            return false;
        };

        let entry_size = key
            .len()
            .saturating_add(value.len())
            .saturating_add(SIZEOF_U16 * 3);

        let block_is_full = self.estimated_size().saturating_add(entry_size) > self.block_size;
        if !self.is_empty() && block_is_full {
            return false;
        }

        let Ok(offset) = u16::try_from(self.data.len()) else {
            return false;
        };

        self.offsets.push(offset);
        let overlap = self.compute_overlap(key);

        // Encode key overlap.
        self.data.put_u16(overlap);
        // Encode key length
        self.data.put_u16(key_len - overlap);
        // Encode key content
        self.data.put(&key.raw_ref()[usize::from(overlap)..]);
        // Encode value length
        self.data.put_u16(value_len);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn estimated_size(&self) -> usize {
        // number of key-value pairs in the blocks + offsets + key-value pairs
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }
}
