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

use std::sync::Arc;

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

        while iter.is_valid() {
            println!(
                "{}->{}",
                String::from_utf8_lossy(iter.key().raw_ref()),
                String::from_utf8_lossy(iter.value())
            );
            iter.next().unwrap();
        }
    }
}
