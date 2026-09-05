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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }

        for i in 0..snapshot.levels.len() - 1 {
            let (cur, cur_sst_ids) = &snapshot.levels[i];
            let (lower_level, lower_sst_ids) = &snapshot.levels[i + 1];

            if !cur_sst_ids.is_empty()
                && (lower_sst_ids.len() / cur_sst_ids.len()) * 100 < self.options.size_ratio_percent
            {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(*cur),
                    upper_level_sst_ids: cur_sst_ids.clone(),
                    lower_level: *lower_level,
                    lower_level_sst_ids: lower_sst_ids.clone(),
                    is_lower_level_bottom_level: *lower_level == snapshot.levels.len() - 1,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_snapshot = snapshot.clone();
        let mut del = Vec::<usize>::new();

        let mut upper_sst_map = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper_level) = task.upper_level {
            let idx = upper_level - 1;

            new_snapshot.levels[idx].1.retain(|id| {
                let found = upper_sst_map.remove(id);
                if found {
                    del.push(*id);
                }
                !found
            });
        } else {
            new_snapshot.l0_sstables.retain(|id| {
                let found = upper_sst_map.remove(id);
                if found {
                    del.push(*id);
                }
                !found
            });
        }

        del.extend(&task.lower_level_sst_ids);
        new_snapshot.levels[task.lower_level - 1].1 = output.to_vec();
        assert!(upper_sst_map.is_empty());

        (new_snapshot, del)
    }
}
