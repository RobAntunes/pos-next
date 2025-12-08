//! Write-Ahead Log (WAL) for crash recovery
//!
//! Design:
//! - Append-only log per shard
//! - 224-byte records (aligned for performance)
//! - CRC32 checksum per record
//! - Periodic fsync (batched for performance)

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// Account definition (duplicated to avoid circular dependency)
#[repr(C, align(128))]
#[derive(Copy, Clone, Debug)]
pub struct Account {
    pub pubkey: [u8; 32],
    pub balance: u64,
    pub nonce: u64,
    pub last_modified: u64,
    pub _padding: [u8; 80],
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

const RECORD_SIZE: usize = 224;
const MAGIC: u32 = 0xDEADBEEF;

#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum RecordType {
    Insert = 1,
    Update = 2,
    Checkpoint = 3,
}

impl RecordType {
    fn from_u32(val: u32) -> Option<Self> {
        match val {
            1 => Some(RecordType::Insert),
            2 => Some(RecordType::Update),
            3 => Some(RecordType::Checkpoint),
            _ => None,
        }
    }
}

#[repr(C)]
struct WalRecord {
    magic: u32,              // 4 bytes - magic number for validation
    timestamp: u64,          // 8 bytes - Unix timestamp
    record_type: u32,        // 4 bytes - RecordType
    checksum: u32,           // 4 bytes - CRC32 of account data
    account: Account,        // 128 bytes
    _padding: [u8; 76],      // 76 bytes padding (4+8+4+4+128+76 = 224)
}

impl WalRecord {
    fn new(record_type: RecordType, account: Account) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let checksum = Self::compute_checksum(&account);
        
        Self {
            magic: MAGIC,
            timestamp,
            record_type: record_type as u32,
            checksum,
            account,
            _padding: [0u8; 76],
        }
    }

    fn compute_checksum(account: &Account) -> u32 {
        let bytes = unsafe {
            std::slice::from_raw_parts(
                account as *const Account as *const u8,
                std::mem::size_of::<Account>(),
            )
        };
        crc32fast::hash(bytes)
    }

    fn validate(&self) -> bool {
        if self.magic != MAGIC {
            return false;
        }
        let computed = Self::compute_checksum(&self.account);
        computed == self.checksum
    }

    fn to_bytes(&self) -> [u8; RECORD_SIZE] {
        unsafe { std::ptr::read(self as *const WalRecord as *const [u8; RECORD_SIZE]) }
    }

    fn from_bytes(bytes: &[u8; RECORD_SIZE]) -> Self {
        unsafe { std::ptr::read(bytes as *const [u8; RECORD_SIZE] as *const WalRecord) }
    }
}

pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    records_since_sync: usize,
    sync_threshold: usize,
}

impl WalWriter {
    pub fn new(data_dir: &Path, shard_id: usize) -> Result<Self, String> {
        let path = data_dir.join(format!("shard_{:03}.wal", shard_id));
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("Failed to open WAL: {}", e))?;

        Ok(Self {
            writer: BufWriter::with_capacity(1024 * 1024, file), // 1MB buffer
            path,
            records_since_sync: 0,
            sync_threshold: 1000, // Sync every 1000 records
        })
    }

    pub fn append(&mut self, record_type: RecordType, account: &Account) -> Result<(), String> {
        let record = WalRecord::new(record_type, *account);
        let bytes = record.to_bytes();
        
        self.writer
            .write_all(&bytes)
            .map_err(|e| format!("Failed to write WAL record: {}", e))?;

        self.records_since_sync += 1;
        
        // Periodic sync for durability
        if self.records_since_sync >= self.sync_threshold {
            self.sync()?;
        }

        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), String> {
        self.writer
            .flush()
            .map_err(|e| format!("Failed to flush WAL: {}", e))?;
        
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| format!("Failed to fsync WAL: {}", e))?;
        
        self.records_since_sync = 0;
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), String> {
        // Sync any pending writes
        self.sync()?;
        
        // Write checkpoint marker
        let checkpoint_account = Account::default();
        let record = WalRecord::new(RecordType::Checkpoint, checkpoint_account);
        let bytes = record.to_bytes();
        
        self.writer
            .write_all(&bytes)
            .map_err(|e| format!("Failed to write checkpoint: {}", e))?;
        
        self.sync()?;
        Ok(())
    }

    pub fn truncate(&mut self) -> Result<(), String> {
        // Sync and close current file
        self.sync()?;
        
        // Truncate the file
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to truncate WAL: {}", e))?;
        
        // Reopen for appending
        self.writer = BufWriter::with_capacity(1024 * 1024, file);
        self.records_since_sync = 0;
        
        Ok(())
    }
}

pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    pub fn new(data_dir: &Path, shard_id: usize) -> Result<Option<Self>, String> {
        let path = data_dir.join(format!("shard_{:03}.wal", shard_id));
        
        if !path.exists() {
            return Ok(None);
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| format!("Failed to open WAL for reading: {}", e))?;

        Ok(Some(Self {
            reader: BufReader::with_capacity(1024 * 1024, file),
        }))
    }

    pub fn replay<F>(&mut self, mut callback: F) -> Result<usize, String>
    where
        F: FnMut(RecordType, Account) -> Result<(), String>,
    {
        let mut count = 0;
        let mut buffer = [0u8; RECORD_SIZE];

        loop {
            match self.reader.read_exact(&mut buffer) {
                Ok(_) => {
                    let record = WalRecord::from_bytes(&buffer);
                    
                    if !record.validate() {
                        return Err(format!("Corrupted WAL record at position {}", count));
                    }

                    if let Some(record_type) = RecordType::from_u32(record.record_type) {
                        if record_type == RecordType::Checkpoint {
                            // Checkpoint marker - all previous records applied
                            count = 0;
                            continue;
                        }
                        
                        callback(record_type, record.account)?;
                        count += 1;
                    } else {
                        return Err(format!("Invalid record type: {}", record.record_type));
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file - normal
                    break;
                }
                Err(e) => {
                    return Err(format!("Failed to read WAL: {}", e));
                }
            }
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_write_read() {
        let dir = tempdir().unwrap();
        
        let mut writer = WalWriter::new(dir.path(), 0).unwrap();
        
        let account = Account {
            pubkey: [1u8; 32],
            balance: 1000,
            nonce: 1,
            last_modified: 123456,
            _padding: [0u8; 80],
        };
        
        writer.append(RecordType::Insert, &account).unwrap();
        writer.sync().unwrap();
        drop(writer);
        
        let mut reader = WalReader::new(dir.path(), 0).unwrap().unwrap();
        let mut replayed_accounts = Vec::new();
        
        reader.replay(|_type, acc| {
            replayed_accounts.push(acc);
            Ok(())
        }).unwrap();
        
        assert_eq!(replayed_accounts.len(), 1);
        assert_eq!(replayed_accounts[0].pubkey, account.pubkey);
        assert_eq!(replayed_accounts[0].balance, account.balance);
    }

    #[test]
    fn test_checkpoint_truncates_replay() {
        let dir = tempdir().unwrap();
        
        let mut writer = WalWriter::new(dir.path(), 0).unwrap();
        
        // Write 3 records
        for i in 0..3 {
            let account = Account {
                pubkey: [i; 32],
                balance: 1000 + i as u64,
                nonce: i as u64,
                last_modified: 123456,
                _padding: [0u8; 80],
            };
            writer.append(RecordType::Insert, &account).unwrap();
        }
        
        // Checkpoint
        writer.checkpoint().unwrap();
        
        // Write 2 more records
        for i in 3..5 {
            let account = Account {
                pubkey: [i; 32],
                balance: 1000 + i as u64,
                nonce: i as u64,
                last_modified: 123456,
                _padding: [0u8; 80],
            };
            writer.append(RecordType::Insert, &account).unwrap();
        }
        
        writer.sync().unwrap();
        drop(writer);
        
        // Replay - should only see 2 records (after checkpoint)
        let mut reader = WalReader::new(dir.path(), 0).unwrap().unwrap();
        let mut replayed_accounts = Vec::new();
        
        reader.replay(|_type, acc| {
            replayed_accounts.push(acc);
            Ok(())
        }).unwrap();
        
        assert_eq!(replayed_accounts.len(), 2);
        assert_eq!(replayed_accounts[0].pubkey, [3; 32]);
        assert_eq!(replayed_accounts[1].pubkey, [4; 32]);
    }
}
