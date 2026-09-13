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

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let lower_level_sst_ids = &snapshot.levels[in_level - 1].1;
        let mut set = HashSet::<usize>::new();
        for sst_id in sst_ids.iter() {
            let upper = &snapshot.sstables[sst_id];
            for lower_sst_id in lower_level_sst_ids {
                let lower = &snapshot.sstables[lower_sst_id];
                if upper.last_key() >= lower.first_key() && upper.first_key() <= lower.last_key() {
                    set.insert(lower.sst_id());
                }
            }
        }

        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();

        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();

        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if &end_key >= first_key && &begin_key <= last_key {
                overlap_ssts.push(*sst_id);
            }
        }

        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut base_level = self.options.max_levels;
        let mut target_level_size = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();
        let mut real_level_size = Vec::with_capacity(self.options.max_levels);
        for i in 0..self.options.max_levels {
            real_level_size.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| snapshot.sstables[x].table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        target_level_size[self.options.max_levels - 1] =
            real_level_size[self.options.max_levels - 1].max(base_level_size_bytes);
        // select base level and compute target level size
        for i in (0..(self.options.max_levels - 1)).rev() {
            let next_level_size = target_level_size[i + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = this_level_size;
            }

            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        // Flush L0 SST is is the top priority
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            let overlap_sst_ids =
                self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: overlap_sst_ids,
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let (mut highest_priority, mut level) = (-1_f32, 0);
        for (i, &target_size) in target_level_size
            .iter()
            .enumerate()
            .take(snapshot.levels.len() - 1)
        {
            let current = &snapshot.levels[i];
            let priority = (real_level_size[i] as f32) / (target_size as f32);

            if priority > highest_priority {
                highest_priority = priority;
                level = current.0;
            }
        }
        if highest_priority > 1.0 {
            let lower_level = level + 1;
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap(); // select oldest sst
            let overlap_sst_ids =
                self.find_overlapping_ssts(snapshot, &[selected_sst], lower_level);
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );
            println!(
                "compaction triggered by priority: {level} , select {selected_sst} for compaction",
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level,
                lower_level_sst_ids: overlap_sst_ids,
                is_lower_level_bottom_level: lower_level == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut del = Vec::<usize>::new();

        let mut upper_sst_map = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper_level) = task.upper_level {
            let idx = upper_level - 1;

            snapshot.levels[idx].1.retain(|id| {
                let found = upper_sst_map.remove(id);
                if found {
                    del.push(*id);
                }
                !found
            });
        } else {
            snapshot.l0_sstables.retain(|id| {
                let found = upper_sst_map.remove(id);
                if found {
                    del.push(*id);
                }
                !found
            });
        }
        assert!(upper_sst_map.is_empty());

        del.extend(&task.lower_level_sst_ids);

        let mut lower_sst_map = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|sst_id| {
                if lower_sst_map.remove(sst_id) {
                    return None;
                }
                Some(*sst_id)
            })
            .collect::<Vec<_>>();
        assert!(lower_sst_map.is_empty());
        new_lower_level_ssts.extend(output);

        if !in_recovery {
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(snapshot.sstables.get(y).unwrap().first_key())
            });
        }

        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;

        (snapshot, del)
    }
}
