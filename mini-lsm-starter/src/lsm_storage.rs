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

use std::collections::HashMap;
use std::fs::{self, File};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::vec;

use anyhow::{Context, Result, anyhow, ensure};
use bytes::Bytes;
use moka::ops::compute::Op;
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
use crate::key::{KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, MemTableIterator, map_bound};
use crate::mvcc::LsmMvccInner;
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
        // sync files created during open i.e. MANIFEST, first wal etc.
        self.inner.sync_dir()?;

        self.flush_notifier.send(())?;
        self.compaction_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thead) = compaction_thread.take() {
            compaction_thead
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            // Only freeze memtable without adding new record to manifest
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
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

    pub fn new_txn(&self) -> Result<()> {
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

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
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

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);

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

        let manifest_path = LsmStorageInner::path_of_manifest_static(path);

        let block_cache = Arc::new(BlockCache::new(1024));
        let mut next_sst_id = 0;
        let manifest = Manifest::create(LsmStorageInner::path_of_manifest_static(path))?;
        if manifest_path.exists() {
            let (_, manifest_records) =
                Manifest::recover(LsmStorageInner::path_of_manifest_static(path))?;

            for record in manifest_records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        if let Some(index) =
                            state.imm_memtables.iter().position(|x| x.id() == sst_id)
                        {
                            state.imm_memtables.remove(index);
                        }

                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(id) => {
                        if options.enable_wal {
                            next_sst_id = next_sst_id.max(id);
                            let memtable = Arc::new(MemTable::create(id));
                            let old_memtable = std::mem::replace(&mut state.memtable, memtable);
                            state.imm_memtables.insert(0, old_memtable);
                        }
                    }
                    ManifestRecord::Compaction(compaction_task, items) => {
                        let (new_state, del) = compaction_controller.apply_compaction_result(
                            &state,
                            &compaction_task,
                            &items,
                            true,
                        );
                        state = new_state;
                        if let Some(max_id) = items.iter().max() {
                            next_sst_id = next_sst_id.max(*max_id);
                        }
                    }
                }
            }

            for &sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&LsmStorageInner::path_of_sst_static(path, sst_id))?,
                )?;
                state.sstables.insert(sst_id, Arc::new(sst));
            }

            if let CompactionController::Leveled(_) = &compaction_controller {
                for i in 0..state.levels.len() {
                    state.levels[i].1.sort_by(|x, y| {
                        state.sstables[x]
                            .first_key()
                            .cmp(state.sstables[y].first_key())
                    });
                }
            }
            next_sst_id += 1;

            if options.enable_wal {
                if state.memtable.id() != 0 {
                    let id = state.memtable.id();
                    state.memtable = Arc::new(MemTable::recover_from_wal(
                        id,
                        Self::path_of_wal_static(path, id),
                    )?);
                }

                for i in 0..state.imm_memtables.len() {
                    let id = state.imm_memtables[i].id();
                    state.imm_memtables[i] = Arc::new(MemTable::recover_from_wal(
                        id,
                        Self::path_of_wal_static(path, id),
                    )?);
                }
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
        }

        if state.memtable.id() == 0 {
            state.memtable = Arc::new(if options.enable_wal {
                MemTable::create_with_wal(0, Self::path_of_wal_static(path, 0))?
            } else {
                MemTable::create(0)
            })
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(next_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(TS_DEFAULT)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        self.state.read().memtable.sync_wal()?;
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub fn get_from_memtable(memtable: &Arc<MemTable>, key: &[u8]) -> (bool, Option<Bytes>) {
        let lower = Bound::Included(KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN));
        let upper = Bound::Included(KeySlice::from_slice_with_ts(key, TS_RANGE_END));
        let iter = memtable.scan(lower, upper);
        if iter.is_valid() {
            return (
                true,
                Some(Bytes::copy_from_slice(iter.value())).filter(|v| !v.is_empty()),
            );
        }

        (false, None)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let (found, val) = Self::get_from_memtable(&snapshot.memtable, key)
            && found
        {
            return Ok(val);
        }

        for imm_memtable in snapshot.imm_memtables.iter() {
            if let (found, val) = Self::get_from_memtable(imm_memtable, key)
                && found
            {
                return Ok(val);
            }
        }

        let h = farmhash::fingerprint32(key);
        let l0_iters = snapshot
            .l0_sstables
            .iter()
            .map(|id| &snapshot.sstables[id])
            .filter(|table| {
                if let Some(bloom) = &table.bloom
                    && !bloom.may_contain(h)
                {
                    return false;
                }

                key_within(
                    key,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                )
            })
            .map(|table| {
                SsTableIterator::create_and_seek_to_key(
                    table.clone(),
                    KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN),
                )
                .map(Box::new)
            })
            .collect::<Result<Vec<_>>>()?;

        let level_iters = snapshot
            .levels
            .iter()
            .map(|(level, sst_ids)| {
                sst_ids
                    .iter()
                    .filter_map(|id| {
                        let table = &snapshot.sstables[id];
                        if let Some(bloom) = &table.bloom
                            && !bloom.may_contain(h)
                        {
                            return None;
                        }

                        if key_within(
                            key,
                            table.first_key().as_key_slice(),
                            table.last_key().as_key_slice(),
                        ) {
                            Some(table.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .map(|sstables| {
                SstConcatIterator::create_and_seek_to_key(
                    sstables,
                    KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN),
                )
                .map(Box::new)
            })
            .collect::<Result<Vec<_>>>()?;

        let iter = TwoMergeIterator::create(
            MergeIterator::create(l0_iters),
            MergeIterator::create(level_iters),
        )?;
        if iter.is_valid() && iter.key().key_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let memtable_reaches_capacity = {
            let mvcc = self.mvcc();
            let _lock = mvcc.write_lock.lock();
            let ts = mvcc.latest_commit_ts() + 1;
            let state = self.state.read();
            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => state.memtable.put(
                        KeySlice::from_slice_with_ts(key.as_ref(), ts),
                        value.as_ref(),
                    )?,
                    WriteBatchRecord::Del(key) => state
                        .memtable
                        .put(KeySlice::from_slice_with_ts(key.as_ref(), ts), b"")?,
                }
            }
            mvcc.update_commit_ts(ts);
            state.memtable.approximate_size() >= self.options.target_sst_size
        };

        if memtable_reaches_capacity {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_manifest_static(path: impl AsRef<Path>) -> PathBuf {
        path.as_ref().join("MANIFEST")
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
        File::open(&self.path)
            .and_then(|file| file.sync_all())
            .context("failed to sync dir")
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(id, self.path_of_wal(id))?)
        } else {
            Arc::new(MemTable::create(id))
        };

        // sync wal of newly created memtable
        if self.options.enable_wal {
            self.sync_dir()?;
        }
        if let Some(manifest) = &self.manifest {
            manifest.add_record(state_lock_observer, ManifestRecord::NewMemtable(id))?;
        }

        self.freeze_memtable_with_memtable(memtable)?;

        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        *guard = Arc::new(snapshot);
        drop(guard);
        old_memtable.sync_wal()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();
        let memtable_to_flush = {
            let guard = self.state.read();
            let Some(memtable) = guard.imm_memtables.last() else {
                return Ok(());
            };

            memtable.clone()
        };

        let sst_id = memtable_to_flush.id();
        let mut builder = SsTableBuilder::new(memtable_to_flush.approximate_size());
        memtable_to_flush.flush(&mut builder)?;
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let removed = snapshot
                .imm_memtables
                .pop()
                .ok_or(anyhow!("failed to pop imm_memtable"))?;
            ensure!(removed.id() == sst_id);

            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }

            println!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            if let Some(manifest) = &self.manifest {
                self.sync_dir()?;
                manifest.add_record(&state_lock, ManifestRecord::Flush(sst_id))?;
            }
            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            fs::remove_file(self.path_of_wal(sst_id))?;
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn map_bound(bound: Bound<&[u8]>) -> Bound<KeySlice> {
        match bound {
            Bound::Included(x) => Bound::Included(KeySlice::from_slice_with_ts(x, TS_RANGE_BEGIN)),
            Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice_with_ts(x, TS_RANGE_END)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let memtable_iter = {
            let mut iters: Vec<Box<MemTableIterator>> = vec![];
            iters.push(Box::new(
                snapshot
                    .memtable
                    .scan(Self::map_bound(lower), Self::map_bound(upper)),
            ));
            for imm_memtable in snapshot.imm_memtables.iter() {
                iters.push(Box::new(
                    imm_memtable.scan(Self::map_bound(lower), Self::map_bound(upper)),
                ));
            }

            MergeIterator::create(iters)
        };

        let l0_iter = {
            let mut iters: Vec<Box<SsTableIterator>> =
                Vec::with_capacity(snapshot.l0_sstables.len());
            for table_id in snapshot.l0_sstables.iter() {
                let table = snapshot.sstables[table_id].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    let sstable_iter = match lower {
                        Bound::Included(start_key) => SsTableIterator::create_and_seek_to_key(
                            table.clone(),
                            KeySlice::from_slice_with_ts(start_key, TS_RANGE_BEGIN),
                        ),
                        Bound::Excluded(start_key) => SsTableIterator::create_and_seek_to_key(
                            table.clone(),
                            KeySlice::from_slice_with_ts(start_key, TS_RANGE_END),
                        ),
                        Bound::Unbounded => {
                            SsTableIterator::create_and_seek_to_first(table.clone())
                        }
                    };
                    iters.push(Box::new(sstable_iter?));
                }
            }

            MergeIterator::create(iters)
        };

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (level, sst_ids) in snapshot.levels.iter() {
            let concat_iter = {
                let ssts = sst_ids
                    .iter()
                    .filter_map(|id| {
                        let table = &snapshot.sstables[id];
                        if range_overlap(
                            lower,
                            upper,
                            table.first_key().as_key_slice(),
                            table.last_key().as_key_slice(),
                        ) {
                            Some(table.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                match lower {
                    Bound::Included(start_key) => SstConcatIterator::create_and_seek_to_key(
                        ssts,
                        KeySlice::from_slice_with_ts(start_key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(start_key) => SstConcatIterator::create_and_seek_to_key(
                        ssts,
                        KeySlice::from_slice_with_ts(start_key, TS_RANGE_END),
                    )?,
                    Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
                }
            };
            level_iters.push(Box::new(concat_iter));
        }

        let merged_iter = TwoMergeIterator::create(
            TwoMergeIterator::create(memtable_iter, l0_iter)?,
            MergeIterator::create(level_iters),
        )?;

        let lsm_iter = LsmIterator::new(merged_iter, map_bound(upper))?;

        Ok(FusedIterator::new(lsm_iter))
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.key_ref() => return false,
        Bound::Included(key) if key < table_begin.key_ref() => return false,
        _ => {}
    }

    match user_begin {
        Bound::Excluded(key) if key >= table_end.key_ref() => return false,
        Bound::Included(key) if key > table_end.key_ref() => return false,
        _ => {}
    }

    true
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.key_ref() <= user_key && user_key <= table_end.key_ref()
}
