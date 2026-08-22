//! Feeds crafted byte buffers into `Block::decode` to see where it fails.
//! `decode_checked` validates the footer, the offsets table and every inline
//! length field, so a malformed buffer should come back as REJECTED. A PANIC
//! line means a value read from the wire reached a slice index unvalidated --
//! that is a missing bounds check, not a demo.
//! Run with: cargo run --example malicious_block

use mini_lsm_starter::block::Block;
use std::panic;

fn hexdump(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn try_decode(name: &str, bytes: &[u8]) {
    println!("--- {name} ---");
    println!("bytes: {}", hexdump(bytes));
    match panic::catch_unwind(|| Block::decode_checked(bytes).map(|_| ())) {
        Ok(Ok(())) => println!("decoded OK"),
        Ok(Err(e)) => println!("REJECTED: {e}"),
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            println!("PANIC (unchecked index -- BUG): {msg}");
        }
    }
    println!();
}

fn main() {
    panic::set_hook(Box::new(|_| {})); // silence default panic backtrace spam

    // A well-formed single-entry block for reference:
    // entry: key_len=2 "k1" value_len=2 "v1"  (8 bytes)
    // offsets: [0x0000]                        (2 bytes)
    // num_of_elements: 1                       (2 bytes)
    let good: Vec<u8> = vec![
        0x00, 0x02, b'k', b'1', 0x00, 0x02, b'v', b'1', // entry
        0x00, 0x00, // offsets[0] = 0
        0x00, 0x01, // count = 1
    ];
    try_decode("well-formed (baseline)", &good);

    // 1) Impossible entry count: same 12-byte buffer, but the trailing count is bumped from 1 to 5.
    let mut impossible_count = good.clone();
    let len = impossible_count.len();
    impossible_count[len - 2..].copy_from_slice(&5u16.to_be_bytes());
    try_decode(
        "impossible entry count (count=5, only 1 entry present)",
        &impossible_count,
    );

    // 2) Non-monotonic / out-of-range stored offset
    let non_monotonic: Vec<u8> = vec![
        0x00, 0x02, b'k', b'1', 0x00, 0x02, b'v', b'1', // entry 0
        0x00, 0x02, b'k', b'2', 0x00, 0x02, b'v', b'2', // entry 1
        0x00, 0x00, 0x00, 0x00, // offsets = [0, 0]  -- not increasing
        0x00, 0x02, // count = 2
    ];
    try_decode("non-monotonic offset (offsets = [0, 0])", &non_monotonic);

    let out_of_range: Vec<u8> = vec![
        0x00, 0x02, b'k', b'1', 0x00, 0x02, b'v', b'1', // 8-byte data section
        0x00, 0x00, 0x00, 0x64, // offsets = [0, 100] -- 100 is past the data
        0x00, 0x02, // count = 2
    ];
    try_decode("out-of-range offset (offsets = [0, 100])", &out_of_range);

    // 3) Length that extends beyond the data section
    let mut oob_len = good.clone();
    oob_len[4..6].copy_from_slice(&0xFFFFu16.to_be_bytes());
    try_decode(
        "length beyond data section (value_len=0xFFFF, 2 bytes present)",
        &oob_len,
    );
}
