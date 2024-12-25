use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::mem_table::MemTable;

#[derive(Clone)]
pub struct LsmStorageState {
    mem_table: Arc<MemTable>,
    imm_memtables: Vec<Arc<MemTable>>,
}

impl LsmStorageState {
    fn create() -> Self {
        LsmStorageState {
            mem_table: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    block_size: usize,
    target_sst_size: usize,
    num_memtable_limit: usize,
    wal_enabled: bool,
    serializable: bool,
}

impl LsmStorageOptions {
    pub fn basic_defaults() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            num_memtable_limit: 50,
            wal_enabled: false,
            serializable: false,
        }
    }
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

pub(crate) struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
}

impl MiniLsm {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }
}

pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    pub(crate) options: LsmStorageOptions,
    pub(crate) next_sst_id: AtomicUsize,
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(value) = snapshot.mem_table.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }
        Ok(None)
    }

    fn freeze_memtable_with_memtable(&self, mem_table: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.mem_table, mem_table);

        snapshot.imm_memtables.insert(0, old_memtable.clone());
        *guard = Arc::new(snapshot);

        drop(guard);

        Ok(())
    }

    fn force_freeze_memtable(&self) -> Result<()> {
        let mem_table_id = self.next_sst_id();
        let mem_table = Arc::new(MemTable::create(mem_table_id));

        self.freeze_memtable_with_memtable(mem_table)?;

        Ok(())
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            //let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            if guard.mem_table.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                //self.force_freeze_memtable(&state_lock)?;
                self.force_freeze_memtable()?;
            }
        }
        Ok(())
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.mem_table.put(key, value)?;
                        size = guard.mem_table.approximate_size();
                        self.try_freeze(size)?;
                    }
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.mem_table.put(key, b"")?;
                        size = guard.mem_table.approximate_size();
                        self.try_freeze(size)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }
}
