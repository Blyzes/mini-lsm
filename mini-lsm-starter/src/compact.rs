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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
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
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_sst = Vec::new();
        // Option tracks SST building state: Some = active, None = not building.
        // Prevents empty SSTs and enables take() for ownership transfer.
        let mut builder = None;

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }

            let builder_inner = builder.as_mut().unwrap();
            if compact_to_bottom_level {
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }

            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();

                let builder = builder.take().unwrap();

                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
            iter.next()?
        }

        // the add operation will not trigger block finish when block is not full, and the builder will not append block data now, so the estimated_size() > 0 is not enough to check whether there is data in the builder
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();

            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }

        Ok(new_sst)
    }

    /// Compacts the SSTables according to the given compaction task.
    ///
    /// Returns the newly created SSTables.
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(id).unwrap().clone(),
                    )?));
                }

                let mut l1_ssts = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                }
                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;

                let iter = TwoMergeIterator::create(MergeIterator::create(l0_iters), l1_iter)?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => {
                let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                for id in upper_level_sst_ids.iter() {
                    upper_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                }

                // let upper_iter;
                // if let Some(upper_level) = upper_level {
                //     let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                // } else {
                let mut upper_iters = Vec::with_capacity(upper_ssts.len());
                for sst in upper_ssts.iter() {
                    upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        sst.clone(),
                    )?));
                }
                let upper_iter = MergeIterator::create(upper_iters);
                // }

                let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                for id in lower_level_sst_ids.iter() {
                    lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                }
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                // The compiler treats associated types as distinct even if both MergeIterator and SstConcatIterator use KeySlice,
                // so TwoMergeIterator requires their KeyType to match exactly.
                let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (_, tier_sst_ids) in tiers {
                    let mut ssts = Vec::with_capacity(tier_sst_ids.len());
                    for id in tier_sst_ids {
                        ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?));
                }
                let iter = MergeIterator::create(iters);
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            _ => unimplemented!(),
        }
    }

    /// Forces a full compaction of L0 and L1 SSTables.
    pub fn force_full_compaction(&self) -> Result<()> {
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };

        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let new_ssts = self.compact(&task)?;

        let mut ids = Vec::with_capacity(new_ssts.len());

        {
            // clone state and modify it
            let state_lock = self.state_lock.lock();
            let mut new_state = self.state.read().as_ref().clone();

            // remove l0 sstables
            for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                new_state.sstables.remove(sst_id);
            }

            for sst in new_ssts {
                ids.push(sst.sst_id());
                new_state.sstables.insert(sst.sst_id(), sst);
            }
            new_state.levels[0].1 = ids.clone();

            let l0_sstables_map = l0_sstables.iter().cloned().collect::<HashSet<usize>>();
            new_state
                .l0_sstables
                .retain(|sst| !l0_sstables_map.contains(sst));

            let mut guard = self.state.write();
            *guard = Arc::new(new_state);
            self.sync_dir()?
        }

        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        println!("force full compaction done, new SSTs: {:?}", ids);

        Ok(())
    }

    /// Triggers a compaction if needed.
    ///
    /// This function checks if a compaction task needs to be scheduled, and if so, performs the compaction,
    /// updates the storage state, and removes obsolete SST files.
    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };

        println!("running compaction task: {:?}", task);

        let sstables = self.compact(&task)?;
        let files_added = sstables.len();
        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();

        let ssts_to_move = {
            let state_lock = self.state_lock.lock();
            let (mut new_state, files_to_move) = self
                .compaction_controller
                .apply_compaction_result(&self.state.read(), &task, &output, false);

            let mut ssts_to_move = Vec::with_capacity(files_to_move.len());

            for file in &files_to_move {
                let res = new_state.sstables.remove(file);
                assert!(res.is_some());
                ssts_to_move.push(res.unwrap());
            }

            for new_sst in sstables {
                let res = new_state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(res.is_none());
            }

            let mut state = self.state.write();
            *state = Arc::new(new_state);
            ssts_to_move
        };

        println!(
            "compaction finished: {} files removed, {} files added",
            ssts_to_move.len(),
            files_added
        );
        // don't remove files inside the state lock to avoid blocking other operations
        for sst in ssts_to_move.iter() {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }

        Ok(())
    }

    /// Spawns a background thread to periodically trigger compaction.
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

    /// Triggers a flush if needed.
    fn trigger_flush(&self) -> Result<()> {
        // use imm_memtables.len() to decide whether to flush rather than self.get_memtable_size()
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?
        }
        Ok(())
    }

    /// Spawns a background thread to periodically trigger flush.
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
