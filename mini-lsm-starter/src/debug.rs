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

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::{
    iterators::StorageIterator,
    lsm_storage::{LsmStorageInner, MiniLsm},
    table::{SsTable, SsTableIterator},
};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            println!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}

impl SsTable {
    pub fn dump_structure(self: &Arc<Self>) {
        let mut iter = SsTableIterator::create_and_seek_to_first(self.clone()).unwrap();

        println!(
            "id={} first={} last={}",
            self.sst_id(),
            String::from_utf8_lossy(self.first_key().as_key_slice().raw_ref()),
            String::from_utf8_lossy(self.last_key().as_key_slice().raw_ref()),
        );

        while iter.is_valid() {
            let key = String::from_utf8_lossy(iter.key().raw_ref());
            let value = String::from_utf8_lossy(iter.value());
            println!("{}->{}", key, value);
            iter.next().unwrap();
        }
    }

    pub fn dump_structure_to_file(self: &Arc<Self>, path: impl AsRef<Path>) -> Result<()> {
        let mut file = File::create(path)?;
        let mut iter = SsTableIterator::create_and_seek_to_first(self.clone())?;

        while iter.is_valid() {
            let key = String::from_utf8_lossy(iter.key().raw_ref());
            let value = String::from_utf8_lossy(iter.value());
            writeln!(file, "{}->{}", key, value)?;
            iter.next()?;
        }

        Ok(())
    }
}
