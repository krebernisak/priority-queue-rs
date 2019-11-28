use std::collections::BTreeMap;
use std::convert::TryInto;

pub trait PriorityQueue<Element> {
    /// create a new priority queue.
    fn new() -> Self;
    /// check whether the queue has no elements.
    fn is_empty(&self) -> bool;
    /// returns the size of the queue.
    fn size(&self) -> usize;
    /// returns the highest-priority element but does not modify the queue.
    fn peek(&self) -> Option<Element>;
    /// add an element to the queue with an associated priority.
    fn insert(&mut self, element: Element, priority: u64);
    /// remove the element from the queue that has the highest priority, and return it.
    fn pop(&mut self) -> Option<Element>;
}

type KeyValueStore = BTreeMap<Vec<u8>, Vec<u8>>;

// Additional requirement: the underlying data structure needs to be a key-value stores
// Note: you may simulate other data structure with key-value store
pub struct PriorityQueueImpl(KeyValueStore);

/// transform byte array: &[u8; 4] to u32
fn as_u32_be(array: &[u8; 4]) -> u32 {
    ((array[0] as u32) << 24) |
    ((array[1] as u32) << 16) |
    ((array[2] as u32) <<  8) |
    ((array[3] as u32) <<  0)
}

const KEY_SIZE_BYTES: usize = 4;
const ELEMENT_SIZE_BYTES: usize = 4;

/// returns next (element_size: &[u8], element: &[u8], next_slice: &[u8])
fn next_element(slice: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let (element_size, o1) = slice.split_at(ELEMENT_SIZE_BYTES);
    let element_usize = as_u32_be(element_size.try_into().unwrap()) as usize;
    let (element, o2) = o1.split_at(element_usize);
    return (element_size, element, o2);
}

impl PriorityQueue<Vec<u8>> for PriorityQueueImpl {

    fn new() -> Self {
        PriorityQueueImpl(KeyValueStore::new())
    }

    fn is_empty(&self) -> bool {
        let PriorityQueueImpl(kv_store) = self;
        kv_store.is_empty()
    }

    fn size(&self) -> usize {
        let PriorityQueueImpl(kv_store) = self;
        // iterate over everything and add size for every key
        let mut size: usize = 0;
        for (_, val) in kv_store.iter() {
            let (key_size, _) = val.split_at(4);
            size += as_u32_be(key_size.try_into().unwrap()) as usize;
        }
        size
    }

    fn peek(&self) -> Option<Vec<u8>> {
        if self.is_empty() {
            return None
        }

        let PriorityQueueImpl(kv_store) = self;
        let (_, value) = kv_store.iter().next_back().unwrap();
        let (key_size, mut elements_slice) = value.split_at(KEY_SIZE_BYTES);
        let size = as_u32_be(key_size.try_into().unwrap());

        // loop n steps and return the last element
        let mut n = size;
        loop {
            let (_, element, next_slice) = next_element(elements_slice);
            elements_slice = next_slice;

            n -= 1;
            if n == 0 {
                return Some(element.to_vec());
            }
        }
    }

    /// We store elements in a K.V store where K is the priority.
    /// The underlying K.V store is implemented as BTreeMap that is ordered on keys (priority).
    /// As multiple elements can have the same priority we need to accommodate multiple values (elements) for same key (priority)
    ///
    /// Elements are represented in bytes like this:
    /// [elements_size: &[u8; 4], element_1_size: &[u8; 4], element_1 &[u8], element_2_size: &[u8; 4], element_2 &[u8], ...]
    ///
    /// For example:
    ///   queue.insert(vec![5], 10);
    ///   queue.insert(vec![6,7], 10);
    ///   queue.insert(vec![8,9,10], 10);
    ///
    /// Elements with priority 10 would be represented as following:
    ///   [0,0,0,3,0,0,0,1,5,0,0,0,2,6,7,0,0,0,3,8,9,10]
    /// ->| 3 ELM | u8; 1 |5| u8; 2 |6,7| u8; 3 |8,9,10]
    ///
    fn insert(&mut self, element: Vec<u8>, priority: u64) {
        // panic if element over max size (~2GB)
        if element.len() > u32::max_value() as usize {
            panic!("Element size {:?} greater than MAX: {:?}", element.len(), u32::max_value());
        }

        let PriorityQueueImpl(kv_store) = self;
        let key = priority.to_be_bytes().to_vec();
        let element_size = (element.len() as u32).to_be_bytes().to_vec();
        // insert first element if store !contains key, or append to byte array
        let val =
            if !kv_store.contains_key(&key) {
                let key_size = vec![0,0,0,1];
                key_size.into_iter()
                    .chain(element_size.into_iter())
                    .chain(element.into_iter())
                    .collect()
            } else {
                let old_value: Vec<u8> = kv_store.get(&key).unwrap().to_vec();
                let (old_key_size, other) = old_value.split_at(KEY_SIZE_BYTES);
                let size = as_u32_be(old_key_size.try_into().unwrap());
                let key_size = (size + 1).to_be_bytes().to_vec();
                key_size.into_iter()
                    .chain(other.to_vec().into_iter())
                    .chain(element_size.into_iter())
                    .chain(element.into_iter())
                    .collect()
            };
        kv_store.insert(key, val);
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        if self.is_empty() {
            return None
        }

        let PriorityQueueImpl(kv_store) = self;
        let key: Vec<u8>;
        let value: Vec<u8>;
        {
            // do not fight the borrow checker (immutable borrow short scope)
            let (k, v) = kv_store.iter().next_back().unwrap();
            key = k.to_vec();
            value = v.to_vec();
        }

        let (key_size, mut elements_slice) = value.split_at(KEY_SIZE_BYTES);
        let size = as_u32_be(key_size.try_into().unwrap());
        let mut new_val = (size - 1).to_be_bytes().to_vec();

        // loop n steps and return the last element
        // remove key if size == 1, else remove last element and update key
        let mut n = size;
        loop {
            let (element_size, element, next_slice) = next_element(elements_slice);
            elements_slice = next_slice;

            n -= 1;
            if n == 0 {
                if size == 1 {
                    kv_store.remove(&key.to_vec());
                } else {
                    kv_store.insert(key, new_val);
                }
                return Some(element.to_vec());
            } else {
                new_val = new_val.into_iter()
                    .chain(element_size.to_vec().into_iter())
                    .chain(element.to_vec().into_iter())
                    .collect();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_as_u32_be() {
        assert_eq!(as_u32_be(&[0,0,0,1]), 1);
        assert_eq!(as_u32_be(&[0xf0, 0x9f, 0x8f, 0xb3]), 4036988851);
    }

    #[test]
    fn check_is_empty() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        queue.insert(vec![0], 5);
        assert!(!queue.is_empty());
    }

    #[test]
    fn check_size_empty() {
        let queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());
        assert_eq!(queue.size(), 0);
    }

    #[test]
    fn check_size() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());
        assert_eq!(queue.size(), 0);

        queue.insert(vec![0], 5);
        queue.insert(vec![1], 10);
        queue.insert(vec![2], 3);

        assert!(!queue.is_empty());
        assert_eq!(queue.size(), 3);
    }

    #[test]
    fn check_insert() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        queue.insert(vec![0], 5);
        assert_eq!(queue.size(), 1);
    }

    #[test]
    fn check_insert_many() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        queue.insert(vec![0], 10);
        queue.insert(vec![1], 9);
        queue.insert(vec![2], 8);
        queue.insert(vec![3], 7);
        queue.insert(vec![4], 6);
        queue.insert(vec![5], 5);
        queue.insert(vec![6], 4);
        queue.insert(vec![7], 3);
        queue.insert(vec![8], 2);
        queue.insert(vec![9], 1);
        assert_eq!(queue.size(), 10);
        assert!(!queue.is_empty());
    }

    #[test]
    fn check_insert_duplicate() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        queue.insert(vec![4], 10);
        queue.insert(vec![5], 8);
        queue.insert(vec![6], 10);
        assert_eq!(queue.size(), 3);
        assert!(!queue.is_empty());
        assert_eq!(queue.peek(), Some(vec![6]));
        assert_eq!(queue.pop(), Some(vec![6]));
        assert_eq!(queue.peek(), Some(vec![4]));
        assert_eq!(queue.pop(), Some(vec![4]));
        assert_eq!(queue.size(), 1);
        assert_eq!(queue.pop(), Some(vec![5]));
        assert!(queue.is_empty());
        assert_eq!(queue.size(), 0);
    }

    #[test]
    fn check_peek_empty() {
        let queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());
        assert_eq!(queue.peek(), None);
    }

    #[test]
    fn check_peek() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        queue.insert(vec![0], 5);
        assert_eq!(queue.peek(), Some(vec![0]));
        assert!(!queue.is_empty());
        assert_eq!(queue.size(), 1);
        assert_eq!(queue.peek(), Some(vec![0]));
    }

    #[test]
    fn check_pop_empty() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        assert_eq!(queue.size(), 0);
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn check_pop() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        queue.insert(vec![0], 5);
        assert_eq!(queue.pop(), Some(vec![0]));
        assert!(queue.is_empty());
        assert_eq!(queue.size(), 0);
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn it_works() {
        let mut queue = PriorityQueueImpl::new();
        assert!(queue.is_empty());

        queue.insert(vec![0], 5);
        assert!(!queue.is_empty());
        assert_eq!(queue.peek(), Some(vec![0]));

        queue.insert(vec![1], 10);
        queue.insert(vec![2], 3);
        queue.insert(vec![3], 4);
        queue.insert(vec![4], 6);

        assert_eq!(queue.pop(), Some(vec![1]));
        assert_eq!(queue.pop(), Some(vec![4]));
        assert_eq!(queue.pop(), Some(vec![0]));
        assert_eq!(queue.pop(), Some(vec![3]));
        assert_eq!(queue.pop(), Some(vec![2]));

        assert!(queue.is_empty());
    }

    #[test]
    fn check_new_instances() {
        let mut queue_first = PriorityQueueImpl::new();
        let mut queue_second = PriorityQueueImpl::new();
        assert!(queue_first.is_empty());
        assert!(queue_second.is_empty());

        queue_first.insert(vec![0], 5);
        assert_eq!(queue_first.peek(), Some(vec![0]));
        assert!(!queue_first.is_empty());
        assert!(queue_second.is_empty());

        queue_second.insert(vec![0], 5);
        assert_eq!(queue_first.peek(), Some(vec![0]));
        assert_eq!(queue_second.peek(), Some(vec![0]));
        assert!(!queue_first.is_empty());
        assert!(!queue_second.is_empty());
    }

}
