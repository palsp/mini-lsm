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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

use crate::key::Key;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();

        let mut i: usize = 0;
        for j in 1..self.offsets.len() {
            let end = self.offsets[j] as usize;
            out.put_slice(&self.data[i..end]);
            i = end;
        }

        out.put_slice(&self.data[i..self.data.len()]);

        for &i in self.offsets.iter() {
            out.put_u16(i);
        }

        out.put_u16(self.offsets.len() as u16);

        out.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        let num_of_elements = u16::from_be_bytes(data[len - 2..len].try_into().unwrap()) as usize;

        let mut builder = BlockBuilder::new(65_535);

        let mut i = 0;
        for _ in 0..num_of_elements {
            let key_offset = i + 2;
            let key_len = u16::from_be_bytes(data[i..key_offset].try_into().unwrap()) as usize;
            let key = &data[key_offset..key_offset + key_len];
            let value_len_offset = key_offset + key_len;
            let value_offset = value_len_offset + 2;
            let value_len =
                u16::from_be_bytes(data[value_len_offset..value_offset].try_into().unwrap())
                    as usize;
            let value = &data[value_offset..value_offset + value_len];

            assert!(builder.add(Key::from_slice(key), value), "block overflow");
            i = value_offset + value_len;
        }

        builder.build()
    }
}
