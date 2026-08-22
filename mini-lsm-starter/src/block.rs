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

use anyhow::{Context, Result, ensure};
pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

use crate::key::Key;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

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
        Self::decode_checked(data).expect("invalid block encoding")
    }

    pub fn decode_checked(data: &[u8]) -> Result<Self> {
        let mut builder = BlockBuilder::new(65_535);

        ensure!(data.len() >= SIZEOF_U16, "block footer is truncated");
        let entry_offsets_len =
            u16::from_be_bytes([data[data.len() - 2], data[data.len() - 1]]) as usize;
        ensure!(entry_offsets_len > 0, "block has no entries");
        let offset_size = entry_offsets_len
            .checked_mul(SIZEOF_U16)
            .context("block footer is too large")?;
        let footer_size = offset_size
            .checked_add(SIZEOF_U16)
            .context("block offset table is too large")?;
        ensure!(footer_size <= data.len(), "block offset table is truncated");
        let data_end = data.len() - footer_size;
        let offset_raw = &data[data_end..data.len() - SIZEOF_U16];

        let offsets: Vec<u16> = offset_raw
            .chunks(SIZEOF_U16)
            .map(|x| u16::from_be_bytes([x[0], x[1]]))
            .collect();
        ensure!(
            offsets[0] == 0,
            "first block entry must start at offset zero"
        );
        ensure!(
            offsets.windows(2).all(|pair| pair[0] < pair[1]),
            "block entry offsets are not strictly increasing"
        );

        for (idx, offset) in offsets.iter().enumerate() {
            let entry_start = usize::from(*offset);
            let entry_end = offsets
                .get(idx + 1)
                .map_or(data_end, |offset| usize::from(*offset));
            let entry = &data[entry_start..entry_end];
            // entry >=  key_len + value_len
            ensure!(entry.len() >= 4, "block entry header is truncated");
            let key_len = u16::from_be_bytes([entry[0], entry[1]]) as usize;
            let key_end = SIZEOF_U16
                .checked_add(key_len)
                .context("block key length overflow")?;
            let value_len_end = key_end
                .checked_add(SIZEOF_U16)
                .context("block value header overflow")?;
            ensure!(
                value_len_end <= entry.len(),
                "block key or value length is truncated"
            );
            let value_len = u16::from_be_bytes([entry[key_end], entry[key_end + 1]]) as usize;
            let entry_len = value_len_end
                .checked_add(value_len)
                .context("block value length overflow")?;
            ensure!(entry_len == entry.len(), "block value length is invalid");

            let success = builder.add(
                Key::from_slice(&entry[SIZEOF_U16..key_end]),
                &entry[value_len_end..entry_len],
            );
            ensure!(success, "block is full");
        }

        Ok(builder.build())
    }
}
