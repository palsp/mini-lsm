use crate::lsm_storage::{LsmStorageInner, LsmStorageOptions};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn probe_scratch_bloom_and_prefix() {
    let dir = tempdir().unwrap();
    let storage =
        Arc::new(LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap());
    for batch in 0..6 {
        for i in 0..50 {
            let key = format!("batch{:02}_key_{:05}", batch, i);
            storage.put(key.as_bytes(), b"v").unwrap();
        }
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.force_flush_next_imm_memtable().unwrap();
    }
    assert_eq!(storage.state.read().l0_sstables.len(), 6);

    // absent key, should be rejected by all 6 bloom filters
    let _ = storage.get(b"totally_absent_key_zzz").unwrap();

    // known present / absent
    assert!(storage.get(b"batch03_key_00010").unwrap().is_some());
    assert!(storage.get(b"batch03_key_99999").unwrap().is_none());
}

#[test]
fn probe_scratch_prefix_size() {
    use crate::block::BlockBuilder;
    use crate::key::KeySlice;

    let mut builder = BlockBuilder::new(65536);
    let n = 100;
    for i in 0..n {
        let key = format!("common_shared_prefix_key_that_is_long_{:05}", i);
        let value = format!("value_{:05}", i);
        assert!(builder.add(
            KeySlice::for_testing_from_slice_no_ts(key.as_bytes()),
            value.as_bytes()
        ));
    }
    let block = builder.build();
    let encoded_len = block.encode().len();

    // naive baseline: what size would be if every key stored in full (no overlap),
    // same per-entry header overhead (overlap_len(2) + rest_len(2) + value_len(2)) + offsets(2) + count(2)
    let mut naive = 2; // count
    for i in 0..n {
        let key = format!("common_shared_prefix_key_that_is_long_{:05}", i);
        let value = format!("value_{:05}", i);
        naive += 2 + 2 + key.len() + 2 + value.len() + 2; // overlap+restlen+key+valuelen+value+offset
    }

    println!(
        "PREFIX_SIZE entries={} encoded_with_compression={} naive_no_compression={} saved={}",
        n,
        encoded_len,
        naive,
        naive as i64 - encoded_len as i64
    );
}
