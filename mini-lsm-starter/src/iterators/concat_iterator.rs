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

use anyhow::{Result, ensure};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 1,
                sstables,
            });
        }

        let first = &sstables[0];
        let iter = SsTableIterator::create_and_seek_to_first(first.clone())?;

        Ok(Self {
            current: Some(iter),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let (mut left, mut right) = (0_usize, sstables.len());

        while left < right {
            let mid = left + (right - left) / 2;
            let table = &sstables[mid];

            if table.first_key().as_key_slice().le(&key) && table.last_key().as_key_slice().ge(&key)
            {
                let iter = SsTableIterator::create_and_seek_to_key(table.clone(), key)?;
                return Ok(Self {
                    current: Some(iter),
                    next_sst_idx: mid + 1,
                    sstables,
                });
            }

            if table.first_key().as_key_slice().ge(&key) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        if left >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: left + 1,
                sstables,
            });
        }

        let table = &sstables[left];
        let iter = SsTableIterator::create_and_seek_to_first(table.clone())?;
        Ok(Self {
            current: Some(iter),
            next_sst_idx: left + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(iter) = &self.current
            && iter.is_valid()
        {
            return true;
        }

        false
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = self.current.as_mut()
            && iter.is_valid()
        {
            return iter.next();
        }

        if self.next_sst_idx > self.sstables.len() {
            self.current = None;
            return Ok(());
        }

        let next = &self.sstables[self.next_sst_idx];
        self.current = Some(SsTableIterator::create_and_seek_to_first(next.clone())?);
        self.next_sst_idx += 1;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
