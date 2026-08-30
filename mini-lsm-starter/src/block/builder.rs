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

use crate::key::{KeySlice, KeyVec};

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

    fn find_overlap_len(&self, key: KeySlice) -> u16 {
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
        let mut data = BytesMut::new();
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
            data.put_u16(0); // no overlap key len
            data.put_u16(key.len() as u16);
            data.put_slice(key.into_inner());
        } else {
            let overlap_key_len = self.find_overlap_len(key);
            data.put_u16(overlap_key_len);
            if let Some(rest_key_len) = (key.len() as u16).checked_sub(overlap_key_len) {
                data.put_u16(rest_key_len);
                let rest_key_start = key.len() - (overlap_key_len as usize);
                data.put_slice(&key.into_inner()[(overlap_key_len as usize)..]);
            } else {
                return false;
            }
        }

        data.put_u16(value.len() as u16);
        data.put_slice(value);

        let size = self.data.len() + self.offsets.len() * 2 + 2;
        if !self.is_empty() && size + data.len() + 2 > self.block_size {
            return false;
        }

        let Ok(offset) = u16::try_from(self.data.len()) else {
            return false;
        };

        self.data.append(&mut data.to_vec());
        self.offsets.push(offset);
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
}
