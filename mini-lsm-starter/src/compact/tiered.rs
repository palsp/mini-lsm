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

use std::{
    collections::{HashMap, HashSet},
    ops::{Add, Div},
};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );

        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        if let Some(bottom) = &snapshot.levels.last() {
            let mut upper_size = 0;
            // Check amplification Ratio
            for i in 0..(snapshot.levels.len() - 1) {
                let upper = &snapshot.levels[i];
                upper_size += upper.1.len()
            }

            let ratio = upper_size / bottom.1.len();
            if ratio >= self.options.max_size_amplification_percent / 100 {
                println!(
                    "compaction triggered by space amplification ratio: {}",
                    ratio * 100
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                });
            }

            // Check Size Ratio
            let mut acc: usize = 0;
            for (i, level) in snapshot.levels.iter().enumerate() {
                if acc > 0 {
                    let ratio = ((level.1.len() as f64) * 100.0) / (acc as f64);
                    let threshold = (100 + self.options.size_ratio) as f64;

                    if ratio > threshold && i >= self.options.min_merge_width {
                        println!(
                            "compaction triggered by size ratio: {} > {}",
                            ratio, threshold
                        );
                        return Some(TieredCompactionTask {
                            tiers: snapshot.levels.iter().as_slice()[0..i].to_vec(),
                            bottom_tier_included: false,
                        });
                    }
                }

                acc += level.1.len();
            }

            // Reduce Sorted run
            println!("compaction triggered by reducing sorted runs");
            if let Some(max_merge_width) = self.options.max_merge_width {
                let bottom_tier_included = snapshot.levels.len() <= max_merge_width;
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.iter().as_slice()[0..max_merge_width].to_vec(),
                    bottom_tier_included,
                });
            } else {
                // Select all tier if not set
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                });
            }
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut out = snapshot.clone();
        let mut del = Vec::new();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(tier, sst_ids)| (*tier, sst_ids))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();

        let mut new_tier_added = false;
        for (tier_id, sst_ids) in snapshot.levels.iter() {
            if let Some(sst_to_remove) = tier_to_remove.remove(tier_id) {
                assert_eq!(
                    sst_to_remove, sst_ids,
                    "sst changed after issuing compaction task"
                );
                del.extend(sst_to_remove.iter().copied())
            } else {
                levels.push((*tier_id, sst_ids.clone()));
            }
            if tier_to_remove.is_empty() && !new_tier_added && !output.is_empty() {
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }
        assert!(tier_to_remove.is_empty());

        out.levels = levels;
        (out, del)
    }
}
