//! Manual experiment: does an in-progress scan observe writes made by another
//! thread after the iterator was created?
//!
//! Not a test -- it sleeps between steps so the interleaving is visible.
//! Run with: cargo run --example live_scan

use std::ops::Bound;
use std::thread;
use std::time::Duration;

use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
use tempfile::tempdir;

fn main() {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(dir.path(), LsmStorageOptions::default_for_week1_test()).unwrap();
    storage.put(b"00001", b"233").unwrap();
    storage.put(b"00002", b"2333").unwrap();
    storage.put(b"00003", b"23333").unwrap();

    let storage_b = storage.clone();
    let writer = thread::spawn(move || {
        for i in 4..100 {
            let key = format!("{:05}", i);
            storage_b.put(key.as_bytes(), key.as_bytes()).unwrap();
        }
    });

    let mut iter = storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    let reader = thread::spawn(move || {
        while iter.is_valid() {
            println!(
                "{}: {}",
                String::from_utf8_lossy(iter.key()),
                String::from_utf8_lossy(iter.value()),
            );
            iter.next().unwrap();
            thread::sleep(Duration::from_secs(2));
        }
    });

    reader.join().unwrap();
    writer.join().unwrap();
}
