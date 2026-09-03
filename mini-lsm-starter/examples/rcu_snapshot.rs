use parking_lot::RwLock;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug)]
struct State {
    l0_sstables: Vec<u32>,
}

fn main() {
    let state = Arc::new(RwLock::new(Arc::new(State {
        l0_sstables: vec![1, 2, 3],
    })));

    // take a snapshot, same as compact.rs: Arc::clone(&guard)
    let snapshot = {
        let guard = state.read();
        Arc::clone(&guard)
    };
    println!("snapshot before write: {:?}", snapshot.l0_sstables);

    // another thread does the RCU write: clone -> mutate copy -> swap Arc
    let state_writer = Arc::clone(&state);
    let writer = thread::spawn(move || {
        let mut guard = state_writer.write();
        let mut new_state = guard.as_ref().clone();
        new_state.l0_sstables.push(99);
        *guard = Arc::new(new_state);
        println!("writer: pushed 99, RwLock now points to new allocation");
    });
    writer.join().unwrap();

    thread::sleep(Duration::from_millis(50));

    // old snapshot: unchanged, still points at the original allocation
    println!("snapshot after write:  {:?}", snapshot.l0_sstables);

    // fresh read: sees the new allocation
    let fresh = {
        let guard = state.read();
        Arc::clone(&guard)
    };
    println!("fresh read after write: {:?}", fresh.l0_sstables);

    // proof: different heap allocations, different Arc pointers
    println!(
        "snapshot ptr == fresh ptr? {}",
        Arc::ptr_eq(&snapshot, &fresh)
    );
}
