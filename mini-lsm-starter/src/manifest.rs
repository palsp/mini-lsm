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

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result, ensure};
use nom::ExtendInto;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new().read(true).open(path)?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut cur = 0;

        let mut manifest_records = Vec::new();
        while cur < buf.len() {
            ensure!(cur + 1 < buf.len(), "manifest len is truncated");
            // read len
            let len = u16::from_be_bytes([buf[cur], buf[cur + 1]]) as usize;
            cur += 2;

            // read record
            ensure!(cur + len < buf.len(), "manifest record is truncated");
            let raw = &buf[cur..(cur + len)];
            cur += len;

            // read checksum
            ensure!(cur + 3 < buf.len(), "manifest checksum is truncated");
            let h = u32::from_be_bytes([buf[cur], buf[cur + 1], buf[cur + 2], buf[cur + 3]]);
            cur += 4;

            ensure!(h == crc32fast::hash(raw), "manifest is corrupted");
            let mut deserializer = serde_json::Deserializer::from_slice(raw);
            let record = ManifestRecord::deserialize(&mut deserializer)?;
            manifest_records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            manifest_records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let record_raw = serde_json::to_vec(&record)?;
        let len = u16::try_from(record_raw.len())?;
        let h = crc32fast::hash(&record_raw);

        let mut buf = Vec::with_capacity(SIZEOF_U16 + record_raw.len() + SIZEOF_U32);

        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&record_raw);
        buf.extend_from_slice(&h.to_be_bytes());
        file.write_all(&buf)?;
        file.sync_all()?;

        Ok(())
    }
}
