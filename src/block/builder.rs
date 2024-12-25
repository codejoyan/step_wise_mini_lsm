use crate::key::{KeySlice, KeyVec};
use crate::Block;
use bytes::BufMut;

pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
    first_key: KeyVec,
}

impl BlockBuilder {
    pub fn create(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 + SIZEOF_U16 * self.offsets.len() + self.data.len()
    }

    fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
        let mut i = 0;
        loop {
            if (i >= first_key.len() || i >= key.len()) {
                break;
            }
            if (first_key.raw_ref()[i] != key.raw_ref()[i]) {
                break;
            }
            i += 1;
        }
        i
    }

    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + SIZEOF_U16 * 3 + key.len() + value.len() > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        let overlap = compute_overlap(self.first_key.as_key_slice, key);
        self.offsets.push(self.data.len());
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.len - overlap) as u16);
        self.data.put(&key.raw_ref[overlap..]);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty();
    }

    pub fn build(&self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
