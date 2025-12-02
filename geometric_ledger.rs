//! Geometric Ledger - Persistent, Crash-Safe, Windows-Compatible

use memmap2::{MmapMut, MmapOptions};
use rayon::prelude::*;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

// Fixed-size account (pad to 128 bytes)
#[repr(C, align(128))]
#[derive(Copy, Clone, Debug)]
pub struct Account {
    pub pubkey: [u8; 32],
    pub balance: u64,
    pub nonce: u64,
    pub last_modified: u64,
    pub _padding: [u8; 80], // 32 + 8 + 8 + 8 + 80 = 136, but align(128) rounds to 128
}

impl Default for Account {
    fn default() -> Self {
        Self {
            pubkey: [0u8; 32],
            balance: 0,
            nonce: 0,
            last_modified: 0,
            _padding: [0u8; 80],
        }
    }
}

const ACCOUNT_SIZE: usize = 128;
const SHARD_COUNT: usize = 16;
// 1M accounts per shard (256 shards × 1M = 256M total capacity)
// With 8-byte alignment (down from 64), uses ~4GB RAM instead of 32GB
const ACCOUNTS_PER_SHARD: usize = 1_000_000;

// Atomic index slot
// OPTIMIZATION: align(64) for cache line alignment - prevents false sharing between adjacent slots
// Each slot gets its own cache line, eliminating CPU coherency traffic during concurrent CAS operations
// Trade-off: Uses more memory (~32GB for 256M slots) but massively reduces contention at high TPS
#[repr(C, align(64))]
struct AtomicSlot {
    data: AtomicU64,
    _padding: [u8; 56], // Pad to 64 bytes (8 + 56 = 64)
}

impl AtomicSlot {
    const EMPTY: u64 = 0;

    fn new() -> Self {
        Self {
            data: AtomicU64::new(Self::EMPTY),
            _padding: [0u8; 56],
        }
    }

    fn pack(hash: u32, offset: u32, distance: u8) -> u64 {
        let occupied = 1u64 << 63;
        let dist = (distance as u64) << 56;
        let off = (offset as u64) << 32;
        occupied | dist | off | (hash as u64)
    }

    fn unpack(&self) -> Option<(u32, u32, u8)> {
        let val = self.data.load(Ordering::Acquire);
        if val == Self::EMPTY {
            return None;
        }
        let hash = (val & 0xFFFF_FFFF) as u32;
        let offset = ((val >> 32) & 0x00FF_FFFF) as u32;
        let distance = ((val >> 56) & 0x7F) as u8;
        Some((hash, offset, distance))
    }

    fn from_u64(val: u64) -> (u32, u32, u8) {
        let hash = (val & 0xFFFF_FFFF) as u32;
        let offset = ((val >> 32) & 0x00FF_FFFF) as u32;
        let distance = ((val >> 56) & 0x7F) as u8;
        (hash, offset, distance)
    }
}

const INDEX_SIZE: usize = ACCOUNTS_PER_SHARD * 2;

struct Shard {
    mmap: MmapMut,
    index: Box<[AtomicSlot]>,
    account_count: AtomicU32,
    write_count: AtomicU64,
}

impl Shard {
    fn open(data_dir: &Path, shard_id: usize) -> Result<Self, String> {
        let path = data_dir.join(format!("shard_{:03}.bin", shard_id));
        let size = (ACCOUNTS_PER_SHARD * ACCOUNT_SIZE) as u64;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| format!("Failed to open shard file: {}", e))?;

        if file.metadata().map_err(|e| e.to_string())?.len() < size {
            file.set_len(size)
                .map_err(|e| format!("Failed to set length: {}", e))?;
        }

        let mmap = unsafe {
            MmapOptions::new()
                .len(size as usize)
                .map_mut(&file)
                .map_err(|e| format!("Failed to mmap: {}", e))?
        };

        let mut index_vec = Vec::with_capacity(INDEX_SIZE);
        for _ in 0..INDEX_SIZE {
            index_vec.push(AtomicSlot::new());
        }
        let index = index_vec.into_boxed_slice();

        let mut shard = Self {
            mmap,
            index,
            account_count: AtomicU32::new(0),
            write_count: AtomicU64::new(0),
        };

        shard.rebuild_index()?;
        Ok(shard)
    }

    fn rebuild_index(&mut self) -> Result<(), String> {
        let mut count = 0u32;
        for offset in 0..ACCOUNTS_PER_SHARD {
            let account = self.read_account(offset);
            if account.pubkey != [0u8; 32] {
                let hash = hash_pubkey(&account.pubkey);
                let mut idx = (hash as usize) % INDEX_SIZE;
                let mut distance = 0u8;
                loop {
                    let slot = &self.index[idx];
                    if slot.data.load(Ordering::Relaxed) == AtomicSlot::EMPTY {
                        slot.data.store(
                            AtomicSlot::pack(hash, offset as u32, distance),
                            Ordering::Relaxed,
                        );
                        count += 1;
                        break;
                    }
                    idx = (idx + 1) % INDEX_SIZE;
                    distance += 1;
                    if distance > 127 {
                        break;
                    }
                }
            }
        }
        self.account_count.store(count, Ordering::Relaxed);
        Ok(())
    }

    fn insert(&self, pubkey: &[u8; 32], account: &Account) -> Result<(), String> {
        let hash = hash_pubkey(pubkey);
        let mut idx = (hash as usize) % INDEX_SIZE;
        let mut distance = 0u8;

        loop {
            let slot = &self.index[idx];
            let current = slot.data.load(Ordering::Acquire);

            if current == AtomicSlot::EMPTY {
                let offset = self.account_count.fetch_add(1, Ordering::Relaxed);
                if offset >= ACCOUNTS_PER_SHARD as u32 {
                    return Err("Shard full".to_string());
                }

                let packed = AtomicSlot::pack(hash, offset, distance);
                if slot
                    .data
                    .compare_exchange(
                        AtomicSlot::EMPTY,
                        packed,
                        Ordering::Release,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.write_account(offset as usize, account);
                    self.write_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                continue;
            }

            let (curr_hash, curr_offset, curr_dist) = AtomicSlot::from_u64(current);
            if curr_hash == hash {
                let existing = self.read_account(curr_offset as usize);
                if &existing.pubkey == pubkey {
                    self.write_account(curr_offset as usize, account);
                    self.write_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
            }

            idx = (idx + 1) % INDEX_SIZE;
            distance += 1;
            if distance > 127 {
                return Err("Index full".to_string());
            }
        }
    }

    fn get(&self, pubkey: &[u8; 32]) -> Option<Account> {
        let hash = hash_pubkey(pubkey);
        let mut idx = (hash as usize) % INDEX_SIZE;
        for _ in 0..128 {
            if let Some((slot_hash, offset, _)) = self.index[idx].unpack() {
                if slot_hash == hash {
                    let acc = self.read_account(offset as usize);
                    if &acc.pubkey == pubkey {
                        return Some(acc);
                    }
                }
            } else {
                return None;
            }
            idx = (idx + 1) % INDEX_SIZE;
        }
        None
    }

    fn write_account(&self, offset: usize, account: &Account) {
        let start = offset * ACCOUNT_SIZE;
        let dst = unsafe { self.mmap.as_ptr().add(start) as *mut u8 };
        unsafe {
            std::ptr::copy_nonoverlapping(
                account as *const Account as *const u8,
                dst,
                ACCOUNT_SIZE,
            );
        }
    }

    fn read_account(&self, offset: usize) -> Account {
        let start = offset * ACCOUNT_SIZE;
        let src = unsafe { self.mmap.as_ptr().add(start) };
        unsafe { std::ptr::read_unaligned(src as *const Account) }
    }

    fn flush(&self) -> Result<(), String> {
        self.mmap.flush().map_err(|e| e.to_string())
    }

    /// Batch insert - truly parallel within shard
    /// Phase 1: Classify as updates vs inserts (parallel reads)
    /// Phase 2: Allocate offsets for inserts (single atomic)
    /// Phase 3: Write all accounts to mmap (parallel writes)
    /// Phase 4: Update index entries (parallel CAS)
    fn insert_batch(&self, updates: &[(&[u8; 32], &Account)]) -> Result<(), String> {
        use std::sync::atomic::Ordering;

        if updates.is_empty() {
            return Ok(());
        }

        // Phase 1: Parallel classification - find existing accounts
        let classifications: Vec<_> = updates
            .par_iter()
            .map(|(pk, _)| {
                let hash = hash_pubkey(pk);
                let mut idx = (hash as usize) % INDEX_SIZE;

                // Search for existing entry
                for _ in 0..128 {
                    if let Some((slot_hash, offset, _)) = self.index[idx].unpack() {
                        if slot_hash == hash {
                            let acc = self.read_account(offset as usize);
                            if &acc.pubkey == *pk {
                                return (hash, idx, Some(offset)); // Update existing
                            }
                        }
                    } else {
                        return (hash, idx, None); // Insert new at this slot
                    }
                    idx = (idx + 1) % INDEX_SIZE;
                }
                (hash, idx, None) // Insert new (fallback)
            })
            .collect();

        // Phase 2: Allocate offsets for new accounts (single atomic batch)
        let insert_count = classifications
            .iter()
            .filter(|(_, _, off)| off.is_none())
            .count();
        let base_offset = if insert_count > 0 {
            let offset = self
                .account_count
                .fetch_add(insert_count as u32, Ordering::Relaxed);
            if offset + (insert_count as u32) > ACCOUNTS_PER_SHARD as u32 {
                println!(
                    "❌ Shard full! Offset: {}, Insert: {}, Max: {}",
                    offset, insert_count, ACCOUNTS_PER_SHARD
                );
                return Err("Shard full".to_string());
            }
            offset
        } else {
            0
        };

        // Phase 3: Compute offsets for each update (sequential to assign indices)
        let mut insert_offset_counter = base_offset;
        let offsets: Vec<u32> = classifications
            .iter()
            .map(|(_, _, existing_offset)| {
                if let Some(off) = existing_offset {
                    *off
                } else {
                    let off = insert_offset_counter;
                    insert_offset_counter += 1;
                    off
                }
            })
            .collect();

        // Phase 4: Parallel mmap writes
        updates
            .par_iter()
            .zip(offsets.par_iter())
            .for_each(|((_, acc), offset)| {
                self.write_account(*offset as usize, acc);
            });

        // Phase 5: Parallel index updates with CAS (only for new entries)
        classifications.par_iter().zip(offsets.par_iter()).for_each(
            |((hash, start_idx, existing_offset), offset)| {
                if existing_offset.is_some() {
                    return; // Already in index, mmap write is enough
                }

                // New entry - find slot and CAS it in
                let mut idx = *start_idx;
                let mut distance = 0u8;

                loop {
                    let slot = &self.index[idx];
                    let packed = AtomicSlot::pack(*hash, *offset, distance);

                    if slot
                        .data
                        .compare_exchange(
                            AtomicSlot::EMPTY,
                            packed,
                            Ordering::Release,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }

                    idx = (idx + 1) % INDEX_SIZE;
                    distance += 1;
                    if distance > 127 {
                        break;
                    }
                }
            },
        );

        self.write_count
            .fetch_add(updates.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

pub struct GeometricLedger {
    shards: Vec<Shard>,
}

impl GeometricLedger {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        let data_dir = data_dir.as_ref();
        let shards: Result<Vec<_>, _> = (0..SHARD_COUNT)
            .into_par_iter()
            .map(|i| Shard::open(data_dir, i))
            .collect();

        let shards = shards?;
        let total: u32 = shards
            .iter()
            .map(|s| s.account_count.load(Ordering::Relaxed))
            .sum();
        println!("✅ Geometric Ledger loaded: {} accounts active.", total);

        // Log per-shard usage to debug "Shard full" issues
        for (i, shard) in shards.iter().enumerate() {
            let count = shard.account_count.load(Ordering::Relaxed);
            if count > ACCOUNTS_PER_SHARD as u32 * 9 / 10 {
                println!(
                    "⚠️  Shard #{} is {}% full ({}/{})",
                    i,
                    count * 100 / ACCOUNTS_PER_SHARD as u32,
                    count,
                    ACCOUNTS_PER_SHARD
                );
            }
        }

        Ok(Self { shards })
    }

    pub fn update_batch(&self, updates: &[([u8; 32], Account)]) -> Result<(), String> {
        let mut buckets: Vec<Vec<_>> = (0..SHARD_COUNT).map(|_| Vec::new()).collect();
        for (pk, acc) in updates {
            buckets[pk[0] as usize % SHARD_COUNT].push((pk, acc));
        }

        // Use batch insert for each shard (parallel across shards)
        buckets.par_iter().enumerate().try_for_each(|(i, items)| {
            if !items.is_empty() {
                self.shards[i].insert_batch(items)?;
            }
            Ok::<_, String>(())
        })
    }

    pub fn get(&self, pubkey: &[u8; 32]) -> Option<Account> {
        self.shards[pubkey[0] as usize % SHARD_COUNT].get(pubkey)
    }

    pub fn mint(&self, pubkey: [u8; 32], amount: u64) {
        let mut acc = self.get(&pubkey).unwrap_or_else(|| Account {
            pubkey,
            ..Default::default()
        });
        acc.balance = acc.balance.saturating_add(amount);
        let _ = self.update_batch(&[(pubkey, acc)]);
    }

    pub fn snapshot(&self) -> Result<(), String> {
        self.shards.par_iter().try_for_each(|s| s.flush())
    }

    pub fn stats(&self) -> LedgerStats {
        let total_accounts = self
            .shards
            .iter()
            .map(|s| s.account_count.load(Ordering::Relaxed))
            .sum();
        let total_writes = self
            .shards
            .iter()
            .map(|s| s.write_count.load(Ordering::Relaxed))
            .sum();

        LedgerStats {
            total_accounts,
            total_writes,
            shard_count: SHARD_COUNT,
            capacity: (SHARD_COUNT * ACCOUNTS_PER_SHARD) as u64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LedgerStats {
    pub total_accounts: u32,
    pub total_writes: u64,
    pub shard_count: usize,
    pub capacity: u64,
}

fn hash_pubkey(pubkey: &[u8; 32]) -> u32 {
    let h = blake3::hash(pubkey);
    u32::from_le_bytes(h.as_bytes()[..4].try_into().unwrap())
}
