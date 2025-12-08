//! Geometric Ledger - Persistent, Crash-Safe, Windows-Compatible

use memmap2::{MmapMut, MmapOptions};
use rayon::prelude::*;
use std::fs::{self, OpenOptions, File};
use std::io::{Write, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;

use crate::wal::{WalWriter, WalReader, RecordType};

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
// Removed constants to support dynamic configuration
// const SHARD_COUNT: usize = 16;
// const ACCOUNTS_PER_SHARD: usize = 2_000_000;

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

// Index size is dynamic based on accounts_per_shard
// const INDEX_SIZE: usize = ACCOUNTS_PER_SHARD * 2;

struct Shard {
    mmap: MmapMut,
    index: Box<[AtomicSlot]>,
    account_count: AtomicU32,
    write_count: AtomicU64,
    accounts_per_shard: usize,
    index_size: usize,
    wal: Option<RwLock<WalWriter>>,
    data_path: PathBuf,
    shard_id: usize,
}

impl Shard {
    fn open(data_dir: &Path, shard_id: usize, accounts_per_shard: usize, enable_wal: bool) -> Result<Self, String> {
        let path = data_dir.join(format!("shard_{:03}.bin", shard_id));
        let size = (accounts_per_shard * ACCOUNT_SIZE) as u64;
        let index_size = accounts_per_shard * 2;

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

        // Try to load persisted index
        let (index, needs_rebuild) = Self::try_load_index(data_dir, shard_id, index_size)?;

        // Initialize WAL if enabled
        let wal = if enable_wal {
            Some(RwLock::new(WalWriter::new(data_dir, shard_id)?))
        } else {
            None
        };

        let mut shard = Self {
            mmap,
            index,
            account_count: AtomicU32::new(0),
            write_count: AtomicU64::new(0),
            accounts_per_shard,
            index_size,
            wal,
            data_path: data_dir.to_path_buf(),
            shard_id,
        };

        // Replay WAL if exists
        if enable_wal {
            shard.replay_wal()?;
        }

        // Rebuild index if needed
        if needs_rebuild {
            shard.rebuild_index()?;
        } else {
            // Count accounts from index
            let count = shard.count_accounts_from_index();
            shard.account_count.store(count, Ordering::Relaxed);
        }

        Ok(shard)
    }

    fn try_load_index(data_dir: &Path, shard_id: usize, index_size: usize) -> Result<(Box<[AtomicSlot]>, bool), String> {
        let index_path = data_dir.join(format!("index_{:03}.bin", shard_id));
        let marker_path = data_dir.join(".clean_shutdown");

        // Check if we have a clean shutdown and persisted index
        if marker_path.exists() && index_path.exists() {
            println!("ℹ️  Loading persisted index for shard {}...", shard_id);
            
            let mut file = File::open(&index_path)
                .map_err(|e| format!("Failed to open index file: {}", e))?;
            
            // Read checksum
            let mut checksum_bytes = [0u8; 4];
            file.read_exact(&mut checksum_bytes)
                .map_err(|e| format!("Failed to read checksum: {}", e))?;
            let stored_checksum = u32::from_le_bytes(checksum_bytes);
            
            // Read index data
            let mut index_bytes = vec![0u8; index_size * 8];
            file.read_exact(&mut index_bytes)
                .map_err(|e| format!("Failed to read index data: {}", e))?;
            
            // Verify checksum
            let computed_checksum = crc32fast::hash(&index_bytes);
            if computed_checksum != stored_checksum {
                println!("⚠️  Index checksum mismatch, will rebuild");
                return Ok((Self::create_empty_index(index_size), true));
            }
            
            // Convert bytes to AtomicSlot array
            let mut index_vec = Vec::with_capacity(index_size);
            for i in 0..index_size {
                let offset = i * 8;
                let val = u64::from_le_bytes([
                    index_bytes[offset], index_bytes[offset+1], index_bytes[offset+2], index_bytes[offset+3],
                    index_bytes[offset+4], index_bytes[offset+5], index_bytes[offset+6], index_bytes[offset+7],
                ]);
                let slot = AtomicSlot {
                    data: AtomicU64::new(val),
                    _padding: [0u8; 56],
                };
                index_vec.push(slot);
            }
            
            println!("✅ Loaded persisted index for shard {}", shard_id);
            Ok((index_vec.into_boxed_slice(), false))
        } else {
            Ok((Self::create_empty_index(index_size), true))
        }
    }

    fn create_empty_index(index_size: usize) -> Box<[AtomicSlot]> {
        let mut index_vec = Vec::with_capacity(index_size);
        for _ in 0..index_size {
            index_vec.push(AtomicSlot::new());
        }
        index_vec.into_boxed_slice()
    }

    fn persist_index(&self) -> Result<(), String> {
        let index_path = self.data_path.join(format!("index_{:03}.bin", self.shard_id));
        
        // Collect index data as bytes
        let mut index_bytes = Vec::with_capacity(self.index_size * 8);
        for slot in self.index.iter() {
            let val = slot.data.load(Ordering::Relaxed);
            index_bytes.extend_from_slice(&val.to_le_bytes());
        }
        
        // Compute checksum
        let checksum = crc32fast::hash(&index_bytes);
        
        // Write to file
        let mut file = File::create(&index_path)
            .map_err(|e| format!("Failed to create index file: {}", e))?;
        
        file.write_all(&checksum.to_le_bytes())
            .map_err(|e| format!("Failed to write checksum: {}", e))?;
        
        file.write_all(&index_bytes)
            .map_err(|e| format!("Failed to write index data: {}", e))?;
        
        file.sync_all()
            .map_err(|e| format!("Failed to sync index file: {}", e))?;
        
        Ok(())
    }

    fn count_accounts_from_index(&self) -> u32 {
        self.index.iter()
            .filter(|slot| slot.data.load(Ordering::Relaxed) != AtomicSlot::EMPTY)
            .count() as u32
    }

    fn replay_wal(&mut self) -> Result<(), String> {
        if let Some(mut reader) = WalReader::new(&self.data_path, self.shard_id)? {
            println!("ℹ️  Replaying WAL for shard {}...", self.shard_id);
            let count = reader.replay(|_record_type, wal_account| {
                // Convert wal::Account to geometric_ledger::Account (same memory layout)
                let account = unsafe { std::mem::transmute::<crate::wal::Account, Account>(wal_account) };
                // Apply the account update WITHOUT writing to WAL (avoid recursion)
                self.insert_without_wal(&account.pubkey, &account)?;
                Ok(())
            })?;
            println!("✅ Replayed {} WAL records for shard {}", count, self.shard_id);
        }
        Ok(())
    }

    fn insert_without_wal(&self, pubkey: &[u8; 32], account: &Account) -> Result<(), String> {
        let hash = hash_pubkey(pubkey);
        let mut idx = (hash as usize) % self.index_size;
        let mut distance = 0u8;

        loop {
            let slot = &self.index[idx];
            let current = slot.data.load(Ordering::Acquire);

            if current == AtomicSlot::EMPTY {
                let offset = self.account_count.fetch_add(1, Ordering::Relaxed);
                if offset >= self.accounts_per_shard as u32 {
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

            let (curr_hash, curr_offset, _curr_dist) = AtomicSlot::from_u64(current);
            if curr_hash == hash {
                let existing = self.read_account(curr_offset as usize);
                if &existing.pubkey == pubkey {
                    self.write_account(curr_offset as usize, account);
                    self.write_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
            }

            idx = (idx + 1) % self.index_size;
            distance += 1;
            if distance > 127 {
                return Err("Index full".to_string());
            }
        }
    }

    fn rebuild_index(&mut self) -> Result<(), String> {
        let mut count = 0u32;
        for offset in 0..self.accounts_per_shard {
            let account = self.read_account(offset);
            if account.pubkey != [0u8; 32] {
                let hash = hash_pubkey(&account.pubkey);
                let mut idx = (hash as usize) % self.index_size;
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
                    idx = (idx + 1) % self.index_size;
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
        // Write to WAL first (if enabled)
        if let Some(wal_lock) = &self.wal {
            let mut wal = wal_lock.write().unwrap();
            let record_type = if self.get(pubkey).is_some() {
                RecordType::Update
            } else {
                RecordType::Insert
            };
            // Convert geometric_ledger::Account to wal::Account (same memory layout)
            let wal_account = unsafe { std::mem::transmute::<Account, crate::wal::Account>(*account) };
            wal.append(record_type, &wal_account)?;
        }

        let hash = hash_pubkey(pubkey);
        let mut idx = (hash as usize) % self.index_size;
        let mut distance = 0u8;

        loop {
            let slot = &self.index[idx];
            let current = slot.data.load(Ordering::Acquire);

            if current == AtomicSlot::EMPTY {
                let offset = self.account_count.fetch_add(1, Ordering::Relaxed);
                if offset >= self.accounts_per_shard as u32 {
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

            idx = (idx + 1) % self.index_size;
            distance += 1;
            if distance > 127 {
                return Err("Index full".to_string());
            }
        }
    }

    fn get(&self, pubkey: &[u8; 32]) -> Option<Account> {
        let hash = hash_pubkey(pubkey);
        let mut idx = (hash as usize) % self.index_size;
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
            idx = (idx + 1) % self.index_size;
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
        // Sync WAL
        if let Some(wal_lock) = &self.wal {
            let mut wal = wal_lock.write().unwrap();
            wal.sync()?;
        }
        
        // Flush mmap
        self.mmap.flush().map_err(|e| e.to_string())
    }

    fn checkpoint_wal(&self) -> Result<(), String> {
        if let Some(wal_lock) = &self.wal {
            let mut wal = wal_lock.write().unwrap();
            wal.checkpoint()?;
        }
        Ok(())
    }

    fn shutdown(&self) -> Result<(), String> {
        // Flush everything
        self.flush()?;
        
        // Persist index
        self.persist_index()?;
        
        // Checkpoint and truncate WAL (data is now persisted)
        if let Some(wal_lock) = &self.wal {
            let mut wal = wal_lock.write().unwrap();
            wal.checkpoint()?;
            wal.truncate()?;  // Clear WAL after successful checkpoint
        }
        
        Ok(())
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
                let mut idx = (hash as usize) % self.index_size;

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
                    idx = (idx + 1) % self.index_size;
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
            if offset + (insert_count as u32) > self.accounts_per_shard as u32 {
                println!(
                    "❌ Shard full! Offset: {}, Insert: {}, Max: {}",
                    offset, insert_count, self.accounts_per_shard
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

                    idx = (idx + 1) % self.index_size;
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
    pub shard_count: usize,
    pub accounts_per_shard: usize,
    data_dir: PathBuf,
}

impl GeometricLedger {
    pub fn new(
        data_dir: impl AsRef<Path>,
        shard_count: usize,
        accounts_per_shard: usize,
    ) -> Result<Self, String> {
        Self::new_with_wal(data_dir, shard_count, accounts_per_shard, true)
    }

    pub fn new_with_wal(
        data_dir: impl AsRef<Path>,
        shard_count: usize,
        accounts_per_shard: usize,
        enable_wal: bool,
    ) -> Result<Self, String> {
        let data_dir = data_dir.as_ref();
        
        // Remove clean shutdown marker at startup
        let marker_path = data_dir.join(".clean_shutdown");
        if marker_path.exists() {
            std::fs::remove_file(&marker_path).ok();
        }
        
        let shards: Result<Vec<_>, _> = (0..shard_count)
            .into_par_iter()
            .map(|i| Shard::open(data_dir, i, accounts_per_shard, enable_wal))
            .collect();

        let shards = shards?;
        let total: u32 = shards
            .iter()
            .map(|s| s.account_count.load(Ordering::Relaxed))
            .sum();
        println!("✅ Geometric Ledger loaded: {} accounts active.", total);
        println!("ℹ️  ACCOUNTS_PER_SHARD: {}", accounts_per_shard);
        println!("ℹ️  SHARD_COUNT: {}", shard_count);

        // Log per-shard usage to debug "Shard full" issues
        for (i, shard) in shards.iter().enumerate() {
            let count = shard.account_count.load(Ordering::Relaxed);
            if count > accounts_per_shard as u32 * 9 / 10 {
                println!(
                    "⚠️  Shard #{} is {}% full ({}/{})",
                    i,
                    count * 100 / accounts_per_shard as u32,
                    count,
                    accounts_per_shard
                );
            }
        }

        Ok(Self {
            shards,
            shard_count,
            accounts_per_shard,
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn shutdown(&self) -> Result<(), String> {
        println!("ℹ️  Shutting down GeometricLedger...");
        
        // Shutdown all shards in parallel
        self.shards.par_iter().try_for_each(|shard| shard.shutdown())?;
        
        // Create clean shutdown marker
        let marker_path = self.data_dir.join(".clean_shutdown");
        File::create(&marker_path)
            .map_err(|e| format!("Failed to create shutdown marker: {}", e))?;
        
        println!("✅ GeometricLedger shutdown complete");
        Ok(())
    }

    pub fn update_batch(&self, updates: &[([u8; 32], Account)]) -> Result<(), String> {
        let mut buckets: Vec<Vec<_>> = (0..self.shard_count).map(|_| Vec::new()).collect();
        for (pk, acc) in updates {
            buckets[pk[0] as usize % self.shard_count].push((pk, acc));
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
        self.shards[pubkey[0] as usize % self.shard_count].get(pubkey)
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

    pub fn checkpoint_wal(&self) -> Result<(), String> {
        self.shards.par_iter().try_for_each(|s| s.checkpoint_wal())
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
            shard_count: self.shard_count,
            capacity: (self.shard_count * self.accounts_per_shard) as u64,
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
