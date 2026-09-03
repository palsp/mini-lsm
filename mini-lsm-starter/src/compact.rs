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

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use nom::error::ErrorKind::Switch;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        match task {
            CompactionTask::Leveled(leveled_compaction_task) => unimplemented!(),
            CompactionTask::Tiered(tiered_compaction_task) => unimplemented!(),
            CompactionTask::Simple(simple_leveled_compaction_task) => unimplemented!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut ids = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                for id in l0_sstables {
                    ids.push(id);
                }

                for id in l1_sstables {
                    ids.push(id);
                }

                let iters = ids
                    .iter()
                    .map(|&id| {
                        Box::new(
                            SsTableIterator::create_and_seek_to_first(
                                snapshot.sstables[id].clone(),
                            )
                            .unwrap(),
                        )
                    })
                    .collect::<Vec<Box<SsTableIterator>>>();

                let mut merged_iter = MergeIterator::create(iters);

                let mut sstables = Vec::new();
                let mut builder = SsTableBuilder::new(self.options.block_size);

                while merged_iter.is_valid() {
                    if builder.estimated_size() > self.options.target_sst_size {
                        let id = self.next_sst_id();
                        let sstable = builder.build(id, None, self.path_of_sst(id))?;
                        sstables.push(Arc::new(sstable));
                        builder = SsTableBuilder::new(self.options.block_size);
                    }

                    let key = merged_iter.key();
                    let value = merged_iter.value();
                    if !value.is_empty() {
                        builder.add(key, value);
                    }

                    merged_iter.next()?;
                }

                if builder.estimated_size() > 0 {
                    let id = self.next_sst_id();
                    let sstable = builder.build(id, None, self.path_of_sst(id))?;
                    sstables.push(Arc::new(sstable));
                }

                Ok(sstables)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_to_compact, l1_to_compact) = {
            let snapshot = self.state.read();
            (snapshot.l0_sstables.clone(), snapshot.levels[0].1.clone())
        };
        let new_sstables = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_to_compact.clone(),
            l1_sstables: l1_to_compact.clone(),
        })?;

        let old_sstables = {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            let (l0_old, l0_new) = snapshot
                .l0_sstables
                .iter()
                .partition(|&id| l0_to_compact.contains(id));

            let mut old_sstables = HashMap::new();
            for id in l0_old {
                old_sstables.insert(id, snapshot.sstables[&id].clone());
                snapshot.sstables.remove(&id);
            }
            for id in l1_to_compact {
                old_sstables.insert(id, snapshot.sstables[&id].clone());
                snapshot.sstables.remove(&id);
            }

            let levels = new_sstables.iter().map(|table| table.sst_id()).collect();
            for table in new_sstables {
                snapshot.sstables.insert(table.sst_id(), table);
            }

            snapshot.l0_sstables = l0_new;
            snapshot.levels[0] = (1, levels);
            *guard = Arc::new(snapshot);
            old_sstables
        };

        for (id, v) in old_sstables.iter() {
            let path = self.path_of_sst(*id);
            fs::remove_file(path)?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush = {
            let snapshot = self.state.read();
            snapshot.imm_memtables.len() >= self.options.num_memtable_limit
        };

        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
