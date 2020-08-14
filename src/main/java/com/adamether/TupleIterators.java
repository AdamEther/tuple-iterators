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

        Queue<HeapNode> minHeap = new PriorityQueue<>(
                iterators.size(),
                (o1, o2) -> Tuple.compare(o1.getTuple(), o2.getTuple())
        );

        for (Iterator<Tuple> iterator : iterators) {
            if (iterator.hasNext()) {
                minHeap.offer(new HeapNode(iterator, iterator.next()));
            }
        }

        LinkedList<Tuple> mergedTuples = new LinkedList<>();

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
