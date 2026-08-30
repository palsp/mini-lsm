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

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };

        let (first_key, _) = iter.decode_key_at_idx(0);
        iter.first_key = first_key;
        iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    fn decode_key_at_idx(&self, idx: usize) -> (KeyVec, usize) {
        let offset = self.block.offsets[idx] as usize;
        let overlap_key_len_end = offset + 2;
        let overlap_key_len = u16::from_be_bytes(
            self.block.data[offset..overlap_key_len_end]
                .try_into()
                .unwrap(),
        ) as usize;

        let rest_key_len_end = overlap_key_len_end + 2;
        let rest_key_len = u16::from_be_bytes(
            self.block.data[overlap_key_len_end..rest_key_len_end]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut key = KeyVec::new();
        if overlap_key_len > 0 {
            key.append(&self.first_key.raw_ref()[..overlap_key_len]);
        }
        let key_end = rest_key_len_end + rest_key_len;
        key.append(&self.block.data[rest_key_len_end..key_end]);
        (key, key_end)
    }

    fn seek_to_idx(&mut self, idx: usize) {
        let (key, value_len_offset) = self.decode_key_at_idx(idx);
        let value_offset = value_len_offset + 2;
        let value_len = u16::from_be_bytes(
            self.block.data[value_len_offset..value_offset]
                .try_into()
                .unwrap(),
        ) as usize;

        self.key = key;
        self.idx = idx;
        self.value_range = (value_offset, value_offset + value_len)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::create_and_seek_to_first(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut left, mut right) = (0, self.block.offsets.len());
        while left < right {
            let mid = left + (right - left) / 2;
            let (current_key, _) = self.decode_key_at_idx(mid);
            if current_key.as_key_slice() < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if left >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }
        self.seek_to_idx(left);
    }
}
