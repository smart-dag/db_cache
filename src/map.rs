use core::fmt::Debug;
use std::borrow::{Borrow, BorrowMut};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::utils::Backoff;
use rcu_cell::{RcuCell, RcuReader};

use crate::fifo_cache::FifoCache;

const EMPTY_SEQ: usize = ::std::usize::MAX;
const TOMB_SEQ: usize = ::std::usize::MAX - 1;
//const MAX_ACCESS_TIMES: usize = 5;

#[allow(dead_code)]
pub struct RcuHashMap<K: Debug, V: Debug, H = RandomState> {
    items: Vec<self::Entry<K, V>>,
    hasher_factory: H,
    len: usize,
    mask: usize,
    queue: FifoCache<usize, usize>,
}

#[derive(Debug)]
struct Entry<K: Debug, V: Debug> {
    seq: AtomicUsize,
    key: Option<K>,
    value: RcuCell<V>,
}

impl<K: Debug, V: Debug> Default for Entry<K, V> {
    fn default() -> Self {
        Entry {
            seq: AtomicUsize::new(EMPTY_SEQ),
            key: None,
            value: RcuCell::new(None),
        }
    }
}

impl<K: Debug, V: Debug> Entry<K, V> {
    pub fn update(&mut self, seq: usize, key: K, data: V) {
        self.lock();

        let backoff = Backoff::new();
        self.key = Some(key);

        loop {
            if let Some(mut g) = self.value.try_lock() {
                if g.as_ref().is_none() {
                    g.update(Some(data));
                }
                break;
            }

            backoff.spin();
        }

        // release the data
        self.unlock(seq);
    }

    fn lock(&self) {
        let mut cur = self.seq.load(Ordering::Release);
        while cur == TOMB_SEQ || self.seq.compare_and_swap(cur, TOMB_SEQ, Ordering::Relaxed) != cur
        {
            cur = self.seq.load(Ordering::Release);
        }
    }

    fn unlock(&self, seq: usize) {
        assert!(seq != TOMB_SEQ);
        let cur = self.seq.load(Ordering::Release);
        self.seq.compare_and_swap(cur, seq, Ordering::Relaxed);
    }

    // free the entry
    pub fn clear(&mut self) {
        self.lock();

        let backoff = Backoff::new();
        self.key = None;

        loop {
            if let Some(mut g) = self.value.try_lock() {
                if g.as_ref().is_some() {
                    g.update(None);
                }
                break;
            }

            backoff.spin();
        }

        // release the data
        self.unlock(EMPTY_SEQ);
    }

    pub fn is_empty(&self) -> bool {
        self.seq.load(Ordering::Acquire) != EMPTY_SEQ
    }

    pub fn value(&self) -> Option<RcuReader<V>> {
        self.value.read()
    }

    pub fn seq(&self) -> Option<usize> {
        let mut seq = self.seq.load(Ordering::Acquire);
        while seq == TOMB_SEQ {
            seq = self.seq.load(Ordering::Acquire);
        }

        if seq == EMPTY_SEQ {
            return None;
        }

        Some(seq)
    }
}

impl<K, V> RcuHashMap<K, V, RandomState>
where
    K: Hash + Eq + Send + Sync + Debug,
    V: Send + Sync + Debug,
{
    /// Creates a new hashmap using default options.
    pub fn new(capacity: usize) -> RcuHashMap<K, V, RandomState> {
        assert!(capacity > 0);

        let capacity = capacity.next_power_of_two();

        RcuHashMap {
            items: {
                let mut vec = Vec::with_capacity(capacity);
                vec.resize_with(capacity, Default::default);
                vec
            },
            hasher_factory: Default::default(),
            len: 0,
            mask: capacity - 1,
            queue: FifoCache::with_capacity(capacity),
        }
    }

    //  return index of the entry
    fn get_entry<'a, Q: ?Sized>(&self, hash: u64, key: &Q) -> Option<&Entry<K, V>>
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq,
    {
        let mut index = (hash & self.mask as u64) as usize;
        let mut j = 0;
        while index < self.items.len() {
            if let Some(k) = &self.items[index].key {
                if k.borrow() == key {
                    return Some(&self.items[index]);
                }
            }

            j += 1;
            index = (index + j) & self.mask;
        }

        None
    }

    // change seq to normal value after using result
    fn find_entry_mut<'a, Q: ?Sized>(&mut self, hash: u64, key: &Q) -> Option<&mut Entry<K, V>>
    where
        K: BorrowMut<Q> + Hash + Eq,
        Q: Hash + Eq,
    {
        let mut index = (hash & self.mask as u64) as usize;
        let mut j = 0;
        //  return Some(&mut self.items[index]);
        while index < self.items.len() {
            if let Some(k) = &self.items[index].key {
                if k.borrow() == key {
                    // to tell other that we are using this entry
                    if let Some(seq) = self.items[index].seq() {
                        self.queue.insert(index, seq);
                    } else {
                        self.queue.insert(index, 0);
                    }

                    return Some(&mut self.items[index]);
                }
            }

            j += 1;
            index = (index + j) & self.mask;
        }

        None
    }

    // find an emptry entry for insert
    // unlock after using this result
    fn find_empty_entry(&mut self, hash: u64) -> Option<&mut Entry<K, V>> {
        let mut index = (hash & self.mask as u64) as usize;
        let mut j = 0;
        // if self.len > self.mask *
        let prev = index;
        loop {
            if index < self.items.len() && self.items[index].is_empty() {
                return Some(&mut self.items[index]);
            }

            j += 1;
            index = (index + j) & self.mask;

            if prev == index {
                break;
            }
        }

        // remove an item, return the empty item
        // 1) get index from fifo
        // 2) remove items[index]
        // 3) return empty items[index]
        if let Some((index, _)) = self.pop() {
            self.remove_entry(index);
            return Some(&mut self.items[index]);
        }
        None
    }

    fn pop(&self) -> Option<(usize, usize)> {
        loop {
            let (index, seq) = self.queue.pop()?;
            if Some(seq) == self.get_entry_seq(index) {
                return Some((index, seq));
            }

            if self.queue.is_empty() {
                return None;
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> bool {
        let hash = self.hash(&key);
        let entry = self.find_entry_mut(hash, &key);

        if let Some(v) = entry {
            if let Some(seq) = v.seq() {
                v.update(seq + 1, key, value);
            } else {
                v.update(0, key, value);
            }
            return true;
        }

        if let Some(entry) = self.find_empty_entry(hash) {
            entry.update(0, key, value);
            // self.items[0] = entry;
            return true;
        }

        false
        // let mut index = (hash & self.mask as u64) as usize;
        // let mut j = 0;
        // // if self.len > self.mask *
        // let prev = index;
        // loop {
        //     if index < self.items.len() {
        //         self.items[index] = Entry {
        //             seq: AtomicUsize::new(0),
        //             key: Some(key),
        //             value: RcuCell::new(Some(value)),
        //         };
        //         return true;
        //     }

        //     j += 1;
        //     index = (index + j) & self.mask;

        //     if prev == index {
        //         break;
        //     }
        // }
        // false
    }

    pub fn get<'a, Q: ?Sized>(&'a self, key: &Q) -> Option<RcuReader<V>>
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq,
    {
        let key_hash = self.hash(key);
        let index = (key_hash & self.mask as u64) as usize;
        let entry = self.get_entry(key_hash, key)?;

        if let Some(seq) = entry.seq() {
            self.queue.insert(index, seq);
        }

        entry.value()
    }

    pub fn remove<'a, Q: ?Sized>(&'a mut self, key: &Q)
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq,
    {
        let hash = self.hash(key);
        let index = (hash & self.mask as u64) as usize;

        self.remove_entry(index);
    }

    pub fn get_entry_seq(&self, index: usize) -> Option<usize> {
        self.items[index].seq()
    }

    pub fn remove_entry(&mut self, index: usize) {
        assert!(index < self.items.len());
        self.items[index].clear()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn capacity(&self) -> usize {
        self.items.capacity()
    }

    fn hash<Q: ?Sized>(&self, key: &Q) -> u64
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq,
    {
        let mut hasher = self.hasher_factory.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[test]
fn test_len() {
    let map: RcuHashMap<i32, i32> = RcuHashMap::new(20);

    debug_assert!(map.len() == 20, "len = {}", map.len());
}

#[test]
fn test_find_entry_mut() {
    let mut map: RcuHashMap<i32, i32> = RcuHashMap::new(20);

    let hash = map.hash(&2);
    let entry = map.find_entry_mut(hash, &2);

    debug_assert!(entry.is_none(), "value = {:?}", entry);

    debug_assert!(
        entry.as_ref().unwrap().value().map(|v| *v) == Some(2),
        "value = {:?}",
        entry.unwrap().value().map(|v| *v)
    );
    // map.insert(1, 2);
    // assert_eq!(map.get(&1).map(|v| *v), Some(2));
    // assert!(map.get(&2).is_none());

    // map.insert(2, 4);
    // assert_eq!(map.get(&2).map(|v| *v), Some(4));
}
