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

use std::{collections::HashSet, ops::Add};

use rustyline::history;
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
        if let Some(bottom) = snapshot.levels.last() {
            let target_level_size = self.compute_target_size(snapshot, bottom);

            let base_level = target_level_size
                .iter()
                .position(|&x| x > 0)
                .unwrap()
                .add(1);
            if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
                println!("flush L0 SST to base level {}", base_level);

                let overlap_sst_ids =
                    self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, base_level);
                return Some(LeveledCompactionTask {
                    upper_level: None,
                    upper_level_sst_ids: snapshot.l0_sstables.clone(),
                    lower_level: base_level,
                    lower_level_sst_ids: overlap_sst_ids,
                    is_lower_level_bottom_level: base_level == bottom.0,
                });
            }

            let real_level_size = snapshot
                .levels
                .iter()
                .map(|level| compute_size(snapshot, &level.1))
                .collect::<Vec<_>>();

            let (mut highest_priority, mut lower_level, mut upper_level) = (-1_f32, 0, 0);
            for (i, &target_size) in target_level_size
                .iter()
                .enumerate()
                .take(snapshot.levels.len() - 1)
            {
                let current = &snapshot.levels[i];
                let priority =
                    (compute_size(snapshot, &snapshot.levels[i].1) as f32) / (target_size as f32);

                if priority > highest_priority {
                    highest_priority = priority;
                    upper_level = current.0;
                    lower_level = current.0 + 1;
                }
            }
            if highest_priority > 1.0 {
                let selected_sst = snapshot.levels[upper_level - 1]
                    .1
                    .iter()
                    .min()
                    .copied()
                    .unwrap(); // select oldest sst
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
                    "compaction triggered by priority: {upper_level} , select {selected_sst} for compaction",
                );
                return Some(LeveledCompactionTask {
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: vec![selected_sst],
                    lower_level,
                    lower_level_sst_ids: overlap_sst_ids,
                    is_lower_level_bottom_level: lower_level == bottom.0,
                });
            }
        }

        None
    }

    fn compute_target_size(
        &self,
        snapshot: &LsmStorageState,
        bottom: &(usize, Vec<usize>),
    ) -> Vec<u64> {
        let base_level_size = (self.options.base_level_size_mb as u64) * 1024 * 1024;
        let bottom_size = compute_size(snapshot, &bottom.1);
        let is_bottom_level_exceed_base = bottom_size > base_level_size;
        let mut target_size = vec![0; snapshot.levels.len()];

        let mut found_positive_target_below_base = false;
        for (i, level) in snapshot.levels.iter().enumerate().rev() {
            target_size[i] = match (
                level.0,
                is_bottom_level_exceed_base,
                found_positive_target_below_base,
            ) {
                (l_val, true, _) if l_val == bottom.0 => bottom_size,
                (l_val, false, _) if l_val == bottom.0 => base_level_size,
                (_, true, true) => 0,
                (_, true, false) => {
                    let size = target_size[i + 1] / (self.options.level_size_multiplier as u64);
                    if size <= base_level_size {
                        found_positive_target_below_base = true
                    }
                    size
                }
                (_, false, _) => 0,
            };
        }

        target_size
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
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

        new_lower_level_ssts.sort_by(|x, y| {
            snapshot
                .sstables
                .get(x)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(y).unwrap().first_key())
        });

        new_snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;

        (new_snapshot, del)
    }
}

fn compute_size(snapshot: &LsmStorageState, sst_ids: &[usize]) -> u64 {
    sst_ids.iter().fold(0, |acc, sst_id| {
        acc + snapshot.sstables[sst_id].table_size()
    })
}
