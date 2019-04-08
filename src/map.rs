use core::fmt::Debug;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::utils::Backoff;
use rcu_cell::RcuCell;

use crate::fifo_cache::FifoCache;

const EMPTY_SEQ: usize = ::std::usize::MAX;
const TOMB_SEQ: usize = ::std::usize::MAX - 1;

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
    data: RcuCell<(K, V)>,
}

impl<K: Debug, V: Debug> Default for Entry<K, V> {
    fn default() -> Self {
        Entry {
            seq: AtomicUsize::new(EMPTY_SEQ),
            data: RcuCell::new(None),
        }
    }
}

impl<K: Debug, V: Debug> Entry<K, V> {
    pub fn update(&mut self, seq: usize, key: K, data: V) {
        self.lock();

        let backoff = Backoff::new();

        loop {
            if let Some(mut g) = self.data.try_lock() {
                g.update(Some((key, data)));
                break;
            }

            backoff.spin();
        }

        // release the data
        self.unlock(seq);
    }

    fn lock(&self) {
        loop {
            let cur = self.seq.load(Ordering::Relaxed);

            if cur != TOMB_SEQ {
                self.seq.compare_and_swap(cur, TOMB_SEQ, Ordering::Relaxed);
                break;
            }
        }
    }

    fn unlock(&self, seq: usize) {
        assert!(seq != TOMB_SEQ);
        assert_eq!(
            self.seq.compare_and_swap(TOMB_SEQ, seq, Ordering::Relaxed),
            TOMB_SEQ
        );
    }

    // free the entry
    pub fn clear(&mut self) {
        self.lock();

        let backoff = Backoff::new();

        loop {
            if let Some(mut g) = self.data.try_lock() {
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
        self.seq.load(Ordering::Acquire) == EMPTY_SEQ
    }

    pub fn value(&self) -> Option<V>
    where
        V: Clone,
    {
        if let Some(v) = self.data.read() {
            return Some(v.1.clone());
        }

        None
    }

    pub fn key(&self) -> Option<K>
    where
        K: Clone,
    {
        if let Some(v) = self.data.read() {
            return Some(v.0.clone());
        }

        None
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
    K: Hash + Eq + Send + Sync + Debug + Clone,
    V: Send + Sync + Debug + Clone,
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
            if let Some(k) = &self.items[index].key() {
                if k.borrow() == key {
                    return Some(&self.items[index]);
                }
            }

            j += 1;
            index = (index + j) & self.mask;

            if index == self.items.len() - 1 {
                break;
            }
        }

        None
    }

    // find an emptry entry for insert
    // unlock after using this result
    fn find_empty_entry(&mut self, hash: u64) -> Option<&mut Entry<K, V>> {
        let mut index = (hash & self.mask as u64) as usize;
        let mut j = 0;
        loop {
            if self.items[index].is_empty() {
                return Some(&mut self.items[index]);
            }

            j += 1;
            index = (index + j) & self.mask;

            if index >= self.items.len() - 1 {
                break;
            }
        }

        // remove an item, return the empty item
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

        let mut index = (hash & self.mask as u64) as usize;

        let mut j = 0;
        loop {
            if self.items[index].is_empty() {
                self.items[index].update(0, key, value);
                self.queue.insert(index, 0);
                return true;
            } else if let Some(entry) = self.get_entry(hash, &key) {
                if let Some(seq) = entry.seq() {
                    self.items[index].update(seq + 1, key, value);
                    self.queue.insert(index, seq + 1);
                    return true;
                }
            } else if let Some(entry) = self.find_empty_entry(hash) {
                entry.update(0, key, value);
                self.queue.insert(index, 0);
                return true;
            }

            j += 1;
            index = (index + j) & self.mask;
            if index >= self.items.len() - 1 {
                break;
            }
        }

        false
    }

    pub fn get<'a, Q: ?Sized>(&'a self, key: &Q) -> Option<V>
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
    let capacity = 20;

    let map: RcuHashMap<i32, i32> = RcuHashMap::new(capacity);
    let capacity = capacity.next_power_of_two();
    debug_assert!(map.len() == capacity, "len = {}", map.len());
}

#[test]
fn test_entry() {
    let entry: Entry<i32, i32> = Default::default();
    assert_eq!(entry.seq(), None);
}

#[test]
fn test_entry_update() {
    let mut entry: Entry<i32, i32> = Default::default();
    entry.update(0, 1, 2);
    assert_eq!(entry.seq(), Some(0));
    assert_eq!(entry.key(), Some(1));
    assert_eq!(entry.value(), Some(2));

    entry.update(1, 1, 5);
    assert_eq!(entry.seq(), Some(1));
    assert_eq!(entry.key(), Some(1));
    assert_eq!(entry.value(), Some(5));
}

#[test]
fn test_insert() {
    let mut map: RcuHashMap<i32, i32> = RcuHashMap::new(20);

    assert_eq!(map.insert(1, 2), true);
    assert_eq!(map.get(&1), Some(2));

    assert_eq!(map.insert(1, 3), true);
    assert_eq!(map.get(&1), Some(3));

    assert_eq!(map.insert(2, 6), true);
    assert_eq!(map.get(&2), Some(6));
}
