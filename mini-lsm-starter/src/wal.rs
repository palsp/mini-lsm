// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Result, ensure};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        let buf_writer = BufWriter::new(file);

        Ok(Self {
            file: Arc::new(Mutex::new(buf_writer)),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let data = fs::read(&path)?;
        let mut data = data.as_slice();

        while data.remaining() > 0 {
            let mut hasher = crc32fast::Hasher::new();

            ensure!(data.remaining() >= 2, "key_len is truncated");
            let key_len = data.get_u16();
            hasher.update(&key_len.to_be_bytes());

            ensure!(data.remaining() >= key_len as usize, "key is truncated");
            let key = data.copy_to_bytes(key_len as usize);
            hasher.update(key.iter().as_slice());

            ensure!(data.remaining() >= 2, "val_len is truncated");
            let val_len = data.get_u16();
            hasher.update(&val_len.to_be_bytes());

            ensure!(data.remaining() >= val_len as usize, "value is truncated");
            let value = data.copy_to_bytes(val_len as usize);
            hasher.update(value.iter().as_slice());

            let h = data.get_u32();
            ensure!(h == hasher.finalize(), "wal is corrupted");

            skiplist.insert(key, value);
        }

        Self::create(path)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buf = Vec::new();

        let key_len = u16::try_from(key.len())?;
        let val_len = u16::try_from(value.len())?;

        let mut hasher = crc32fast::Hasher::new();

        buf.put_u16(key_len);
        hasher.update(&key_len.to_be_bytes());

        buf.put(key);
        hasher.update(key);

        buf.put_u16(val_len);
        hasher.update(&val_len.to_be_bytes());

        buf.put(value);
        hasher.update(value);

        let h = hasher.finalize();
        buf.extend_from_slice(&h.to_be_bytes());

        let mut writer = self.file.lock();
        writer.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush()?;

        writer.get_mut().sync_all()?;
        Ok(())
    }
}
