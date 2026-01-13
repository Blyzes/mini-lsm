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

// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, create_dir_all};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use farmhash::fingerprint32;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{self, KeySlice, TS_RANGE_BEGIN};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, map_bound, map_key_bound_plus_ts};
use crate::mvcc::LsmMvccInner;
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

// Check whether the user-specified range overlaps with the table's key range.
fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.key_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.key_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.key_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.key_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    user_key >= table_begin.key_ref() && user_key <= table_end.key_ref()
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread.join().map_err(|e| anyhow!("{:?}", e))?;
        }

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread.join().map_err(|e| anyhow!("{:?}", e))?;
        }

        // no need to flush memtables to SSTs as WAL provides persistency
        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?
        }

        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    pub(crate) fn manifest(&self) -> &Manifest {
        self.manifest.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            create_dir_all(path).context("failed to create DB directory")?;
        }
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache
        let mut next_sst_id = 1;

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest;
        let manifest_path = path.join("MANIFEST");
        let mut last_commit_ts = 0;
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id)
                    }
                    ManifestRecord::NewMemtable(x) => {
                        next_sst_id = next_sst_id.max(x);
                        memtables.insert(x);
                    }
                    ManifestRecord::Compaction(compaction_task, output) => {
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &compaction_task,
                            &output,
                            true,
                        );

                        state = new_state;

                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // recover SSTs
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .with_context(|| format!("failed to open SST: {}", table_id))?,
                )?;
                last_commit_ts = last_commit_ts.max(sst.max_ts());
                state.sstables.insert(table_id, Arc::new(sst));
            }

            println!("{} SSTs opened", state.sstables.len());

            next_sst_id += 1;

            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;

                    let max_ts = memtable
                        .map
                        .iter()
                        .map(|x| x.key().ts())
                        .max()
                        .unwrap_or_default();
                    last_commit_ts = last_commit_ts.max(max_ts);

                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }

                println!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?)
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;

            next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: Arc::new(options),
            mvcc: Some(LsmMvccInner::new(last_commit_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.get(key)
    }

    pub fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        // iter of memtables
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(
            Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_END)),
        )));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(
                Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_END)),
            )));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        // iter of L0 sstables
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    return bloom.may_contain(fingerprint32(key));
                }
                return true;
            };
            false
        };

        // we don't know the sequence of sstables in L0, so we have to check all of them
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if keep_table(key, &table) {
                let iter = SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key, key::TS_RANGE_BEGIN),
                )?;
                l0_iters.push(Box::new(iter));
            }
        }

        // All L0 SSTable iterators have been seeked to the first key >= _key.
        // The merge iterator selects the smallest current key among them.
        // If _key exists in any L0 SSTable, the smallest key must be exactly _key.
        let l0_iter = MergeIterator::create(l0_iters);

        // iter of L1-Lmax sstables
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_level, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table_id in level_sst_ids.iter() {
                let table = snapshot.sstables[table_id].clone();
                if keep_table(key, &table) {
                    level_ssts.push(table);
                }
            }
            let level_iter = SstConcatIterator::create_and_seek_to_key(
                level_ssts,
                KeySlice::from_slice(key, TS_RANGE_BEGIN),
            )?;
            level_iters.push(Box::new(level_iter));
        }

        let levels_iter = MergeIterator::create(level_iters);

        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;

        let iter = TwoMergeIterator::create(iter, levels_iter)?;

        // Use `LsmIterator` to skip tombstones; `TwoMergeIterator` preserves them.
        let iter = LsmIterator::new(iter, Bound::Unbounded, read_ts)?;

        if iter.is_valid() && iter.key() == key {
            if iter.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let _lck = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(KeySlice::from_slice(key, ts), value)?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(KeySlice::from_slice(key, ts), b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        self.mvcc().update_commit_ts(ts);
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        // use LsmStorageInner.put() not MemTable.put()
        // if use MemTable.put(), the force_freeze_memtable will not be triggerd
        self.write_batch(&[WriteBatchRecord::Del(key)])?;
        Ok(())
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            {
                let state_lock = self.state_lock.lock();
                let guard = self.state.read();
                if guard.memtable.approximate_size() >= self.options.target_sst_size {
                    drop(guard);
                    self.force_freeze_memtable(&state_lock)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();

        let new_memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        // get snapshot
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();

        // replace snapshot.memtable with new_memtable
        let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);

        snapshot.imm_memtables.insert(0, old_memtable.clone());

        *guard = Arc::new(snapshot);
        drop(guard);
        old_memtable.sync_wal()?;

        self.manifest().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();
        let flush_memtable;

        {
            let guard = self.state.read();
            flush_memtable = guard.imm_memtables.last().expect("no imm memtable").clone();
        }

        let mut builder = SsTableBuilder::new(self.options.block_size);

        flush_memtable.flush(&mut builder)?;

        let sst_id = flush_memtable.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();

            // `LsmStorageState` implements `Clone`
            // `Arc<T>` implements `Deref<Target=T>`
            // `guard: RwLockWriteGuard<Arc<LsmStorageState>>`
            // So `guard.as_ref().clone()` is equivalent to `(*guard.as_ref()).clone()`
            // It clones the inner `LsmStorageState`, producing an independent copy.
            // (Do NOT use `Arc::clone(&guard)` here, because that would only clone the `Arc` itself)
            let mut snapshot: LsmStorageState = guard.as_ref().clone(); // √

            let mem = snapshot.imm_memtables.pop().expect("no imm memtable");
            assert_eq!(mem.id(), sst_id);
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot)
        }

        self.manifest()
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    /// The tombstones is skipped in LsmIterator::new() and LsmIterator::next().
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.scan(lower, upper)
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // iter of memtable and imm_memtables
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        let (begin, end) = map_key_bound_plus_ts(lower, upper, read_ts);
        memtable_iters.push(Box::new(snapshot.memtable.scan(begin, end)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(begin, end)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        // iter of L0 sstables
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;
                        while iter.is_valid() && iter.key().key_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };
                l0_iters.push(Box::new(iter));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        // iter of L1-Lmax sstables
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_level, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table_id in level_sst_ids.iter() {
                let table = snapshot.sstables[table_id].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    level_ssts.push(table);
                }
            }
            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }

        let levels_iter = MergeIterator::create(level_iters);

        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;

        let iter = TwoMergeIterator::create(iter, levels_iter)?;

        let iter = FusedIterator::new(LsmIterator::new(iter, map_bound(upper), read_ts)?);

        Ok(iter)
    }
}
