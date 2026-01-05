// Copyright (c) 2022-2025 Alex Chi Z
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

use std::collections::HashMap;

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
            "should not add L0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // compaction triggered by space amplification ratio
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }

        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;

        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // compaction triggered by size ratio
        let mut size = 0;
        let mut merge_width = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            merge_width += 1;
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = next_level_size as f64 / size as f64;
            if current_size_ratio >= (100.0 + self.options.size_ratio as f64) / 100.0
                && merge_width >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(merge_width)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: merge_width >= snapshot.levels.len(),
                });
            };
        }

        // trying to reduce sorted runs by a minor compaction without respecting size ratio
        let mut num_tiers_to_take =
            (snapshot.levels.len() - self.options.num_tiers + 1).max(self.options.min_merge_width);
        if let Some(max_merge_width) = self.options.max_merge_width {
            num_tiers_to_take = num_tiers_to_take.min(max_merge_width);
        }
        println!("compaction triggered by reducing sorted runs");

        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: num_tiers_to_take >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add L0 ssts in tiered compaction"
        );
        let mut snapshot = snapshot.clone();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();

        let mut levels = Vec::new();
        let mut files_to_move = Vec::new();
        // marker
        let mut new_tier_added = false;

        for (tier_id, files) in snapshot.levels.iter() {
            if let Some(f) = tier_to_remove.remove(tier_id) {
                assert_eq!(f, files, "files changed after issuing compacting task");
                files_to_move.extend(f.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()));
            }

            // Add the new merged tier only once after all source tiers are consumed.
            if tier_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }

        assert!(tier_to_remove.is_empty());

        snapshot.levels = levels;
        (snapshot, files_to_move)
    }
}
