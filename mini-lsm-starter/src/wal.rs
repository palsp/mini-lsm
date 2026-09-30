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

use anyhow::{Error, Result, ensure};
use bytes::{Buf, BufMut, Bytes};
use core::fmt;
use crc32fast::Hasher;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

#[derive(Debug)]
enum WalError {
    Truncated { field: &'static str },
    InvalidLength { field: &'static str },
    ChecksumMismatch,
}

impl std::error::Error for WalError {}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Truncated { field } => {
                write!(f, "{field} is truncated")
            }
            WalError::InvalidLength { field } => {
                write!(f, "invalid {field} length")
            }
            WalError::ChecksumMismatch => {
                write!(f, "checksum mismatch")
            }
        }
    }
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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let original = fs::read(&path)?;
        let mut data = original.as_slice();
        let mut boundary = 0;

        while data.remaining() > 0 {
            if data.remaining() < std::mem::size_of::<u32>() {
                break;
            }

            let size = data.get_u32();
            match Self::recover_batch(size, &mut data) {
                Ok(entries) => {
                    for (key, value) in entries {
                        skiplist.insert(key, value);
                    }
                    boundary = original.len() - data.remaining();
                }
                Err(err) => {
                    // if frame end inside header, body or checksum, discard only incomplete frame
                    if let Some(WalError::Truncated { .. }) = err.downcast_ref::<WalError>() {
                        break;
                    }

                    // A checksum mismatch or invalid key-value length is a corruption
                    return Err(err);
                }
            }
        }

        fs::write(path.as_ref(), &original[..boundary])?;
        Self::create(path)
    }

    fn recover_batch(size: u32, data: &mut impl Buf) -> Result<Vec<(KeyBytes, Bytes)>> {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&size.to_be_bytes());
        let mut remaining = size;
        let mut entries = Vec::<(KeyBytes, Bytes)>::with_capacity(size as usize);
        while data.remaining() > 0 && remaining > 0 {
            ensure!(
                data.remaining() >= std::mem::size_of::<u16>(),
                WalError::Truncated { field: "key_len" }
            );

            let key_len = data.get_u16();
            hasher.update(&key_len.to_be_bytes());

            ensure!(
                data.remaining() >= key_len as usize,
                WalError::InvalidLength { field: "key_len" }
            );

            let key = data.copy_to_bytes(key_len as usize);
            hasher.update(key.iter().as_slice());

            ensure!(
                data.remaining() >= std::mem::size_of::<u64>(),
                WalError::Truncated { field: "ts" }
            );

            let ts = data.get_u64();
            hasher.update(&ts.to_be_bytes());

            ensure!(
                data.remaining() >= std::mem::size_of::<u16>(),
                WalError::Truncated { field: "value_len" }
            );
            let val_len = data.get_u16();
            hasher.update(&val_len.to_be_bytes());

            ensure!(
                data.remaining() >= val_len as usize,
                WalError::InvalidLength { field: "value" }
            );

            let value = data.copy_to_bytes(val_len as usize);
            hasher.update(value.iter().as_slice());

            entries.push((KeyBytes::from_bytes_with_ts(key, ts), value));
            remaining -= 1;
        }

        ensure!(
            data.remaining() >= std::mem::size_of::<u32>(),
            WalError::Truncated { field: "checksum" }
        );
        let checksum = data.get_u32();
        ensure!(checksum == hasher.finalize(), WalError::ChecksumMismatch);

        Ok(entries)
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut buf = Vec::new();
        let mut hasher = crc32fast::Hasher::new();

        let batch_size = u32::try_from(data.len())?;
        buf.put_u32(batch_size);
        hasher.update(&batch_size.to_be_bytes());

        for (key, value) in data.iter() {
            let key_len = u16::try_from(key.key_len())?;
            let val_len = u16::try_from(value.len())?;

            // key_len ( exclude ts len )
            buf.put_u16(key_len);
            hasher.update(&key_len.to_be_bytes());

            // key
            buf.put(key.key_ref());
            hasher.update(key.key_ref());

            // ts
            buf.put_u64(key.ts());
            hasher.update(&key.ts().to_be_bytes());

            // value_len
            buf.put_u16(val_len);
            hasher.update(&val_len.to_be_bytes());

            // value
            buf.put(*value);
            hasher.update(value);
        }

        // checksum
        let h = hasher.finalize();
        buf.extend_from_slice(&h.to_be_bytes());

        let mut writer = self.file.lock();
        writer.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush()?;

        writer.get_mut().sync_all()?;
        Ok(())
    }
}
