use std::collections::BTreeMap;

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
        kv_store.len()
    }

    fn peek(&self) -> Option<Vec<u8>> {
        if self.is_empty() {
            return None
        }
        let PriorityQueueImpl(kv_store) = self;
        let (_, first_value) = kv_store.iter().next_back().unwrap();
        Some(first_value.to_vec())
    }

    fn insert(&mut self, element: Vec<u8>, priority: u64) {
        let PriorityQueueImpl(kv_store) = self;
        let key = priority.to_be_bytes().to_vec();
        // we do not allow duplicates
        if !kv_store.contains_key(&key) {
            kv_store.insert(key, element);
        }
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        if self.is_empty() {
            return None
        }
        let PriorityQueueImpl(kv_store) = self;
        let key: Vec<u8>;
        {
            let (first_key, _) = kv_store.iter().next_back().unwrap();
            key = first_key.to_vec();
        }
        kv_store.remove(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        queue.insert(vec![0], 10);
        queue.insert(vec![1], 8);
        queue.insert(vec![2], 10);
        assert_eq!(queue.size(), 2);
        assert!(!queue.is_empty());
        assert_eq!(queue.peek(), Some(vec![0]));
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
