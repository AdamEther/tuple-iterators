package com.adamether;

import java.util.*;

public class TupleIterators {

    /**
     *  Implementations must return an iterator that represents the linear merge of the iterators in the provided
     *  collection.
     *
     *  The contract for this method requires that callers provide iterators that contain only unique keys within their
     *  own stream, and whose records are returned in ascending order.  It is not required to gracefully handle API misuse.
     */
    static Iterator<Tuple> merge(Collection<Iterator<Tuple>> iterators) {
        if (iterators == null || iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        // Create min heap of total streams size
        // S: O(m * p * q)
        Queue<HeapNode> minHeap = new PriorityQueue<>(
                iterators.size(),
                (o1, o2) -> Tuple.compare(o1.getTuple(), o2.getTuple())
        );

        // Insert first tuples from each iterator into min heap
        // T: O(m) * O(log(m))
        for (Iterator<Tuple> iterator : iterators) {
            if (iterator.hasNext()) {
                minHeap.offer(new HeapNode(iterator, iterator.next()));
            }
        }

        // Create result list witch will contain combined streams maintaining the ordering, and without duplicates.
        // Choosing LinkedList for fastest insert speed.
        // For ArrayList insert time complexity is O(n) in the worst case due to array resizing.
        // S: O(m * n * p * q)
        LinkedList<Tuple> mergedTuples = new LinkedList<>();

        // Run the loop                   | O(m * n)
        // Poll from min heap             | O(log(m))
        // Get the last item from result  | O(1)
        // Call tuple equals              | O(p * q)
        // Insert into result             | O(1)
        // Insert into min heap           | O(log(m))
        // T                              | O(m * n * p * q * log(m))
        while (!minHeap.isEmpty()) {
            HeapNode heapNode = minHeap.poll();
            Tuple minTuple = heapNode.getTuple();

            if (mergedTuples.isEmpty() || !minTuple.equals(mergedTuples.getLast())) {
                mergedTuples.add(minTuple);
            }

            Iterator<Tuple> iterator = heapNode.getIterator();
            if (iterator.hasNext()) {
                minHeap.offer(new HeapNode(iterator, iterator.next()));
            }
        }

        return mergedTuples.iterator();

        // Complexity analyse
        // T: O(m * n * p * q * log(m))
        // S: O(m * n * p * q)

        // Further optimization
        // We can enhance Tuple#equals method time complexity to constant time by comparing hash strings
        // instead of iterating through each value bytes.
        // (Hashes calculated by MD5 / SHA1 algorithm during tuple construction)
    }

    static class HeapNode {
        private final Iterator<Tuple> iterator;
        private final Tuple tuple;

        HeapNode(Iterator<Tuple> iterator, Tuple tuple) {
            this.iterator = iterator;
            this.tuple = tuple;
        }

        Iterator<Tuple> getIterator() {
            return iterator;
        }

        Tuple getTuple() {
            return tuple;
        }
    }
}