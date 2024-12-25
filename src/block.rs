mod builder;

use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offset_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(offset_len as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - SIZEOF_U16 * entry_offsets_len;
        let raw_offset = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = raw_offset.chunks(SIZEOF_U16).map(|x| x.get_u16()).collect();
        let data = &data[0..data_end].to_vec();
        Self { data, offsets };
    }
}
