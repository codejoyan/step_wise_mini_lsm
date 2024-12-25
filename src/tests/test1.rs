use crate::{iterators::StorageIterator, mem_table::MemTable};
use bytes::Bytes;
use std::ops::Bound;

#[test]
fn test_memtable_get() {
    let mem_table = MemTable::create(0);
    let _ = mem_table.put(b"key1", b"value1");
    let _ = mem_table.put(b"key2", b"value2");
    let _ = mem_table.put(b"key1", b"value1`");
    assert_eq!(
        mem_table.get(b"key1"),
        Some(Bytes::copy_from_slice(b"value1`"))
    );
}

#[test]
fn test_memtable_scan() {
    let mem_table = MemTable::create(0);
    let _ = mem_table.put(b"key1", b"value1");
    let _ = mem_table.put(b"key2", b"value2");
    let _ = mem_table.put(b"key3", b"value3");
    let _ = mem_table.put(b"key4", b"value4");
    let _ = mem_table.put(b"key5", b"value5");
    let _ = mem_table.put(b"key6", b"value6");

    let mut iter = mem_table.scan(Bound::Included(b"key2"), Bound::Excluded(b"key5"));
    assert_eq!(iter.key().raw_ref(), b"key2");
    assert_eq!(iter.value(), b"value2");
    assert_eq!(iter.is_valid(), true);
    let _ = iter.next();
    assert_eq!(iter.key().raw_ref(), b"key3");
    assert_eq!(iter.value(), b"value3");
    assert_eq!(iter.is_valid(), true);
    let _ = iter.next();
    assert_eq!(iter.key().raw_ref(), b"key4");
    assert_eq!(iter.value(), b"value4");
    assert_eq!(iter.is_valid(), true);
    let _ = iter.next();
    assert_eq!(iter.is_valid(), false);
}
