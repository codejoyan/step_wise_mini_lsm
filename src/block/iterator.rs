use std::sync::Arc;

use super::Block;
use create::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

pub struct BlockIterator {
    block: Arc<Block>,
    key: KeyVec,
    value_range: (usize, usize),
    idx: usize,
    first_key: KeyVec,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    pub fn key(&self) -> KeySlice {
        assert_debug!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    pub fn value(&self) -> &[u8] {
        assert_debug!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    pub fn next(&mut self) {
        let idx = self.idx + 1;
        self.seek_to(idx);
    }

    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        seek_to(key);
        iter
    }

    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {}

    fn seek_to_offset(&mut self, offset: usize) {
        let entry = self.block.data[offset..];
        let overlap_len = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key = &entry[offset..key_len];
        self.key.clear();
        self.key.append(&self.first_key[..overlap_len]);
        self.key.append(entry[key]);
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_offset_start = offset + SIZEOF_U16 * 3 + key_len;
        let value_offset_end = value_offset_start + value_len;
        self.value_range = (value_offset_start, value_offset_end);
        entry.advance(value_len);
    }

    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[idx] as usize;
        seek_to_offset(offset);
        self.idx = idx;
    }

    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();

        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(!self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => mid = low + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }
}
