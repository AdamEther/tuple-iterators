package com.adamether;

import org.junit.jupiter.api.Test;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeTest {

    protected Iterator<Tuple> merge(Collection<Iterator<Tuple>> iterators) {
        return TupleIterators.merge(iterators);
    }

    @Test
    public void testMergeNone() {
        assertContents(
                merge(emptyList())
        );
    }

    @Test
    public void testMergeEmpty() {
        assertContents(
                merge(asList(emptyIterator(), emptyIterator()))
        );
    }

    @Test
    public void testMergeSingleWithEmpty() {
        List<Tuple> a = asList(
                t(0),
                t(1)
        );
        assertContents(
                merge(asList(a.iterator(), emptyIterator())),
                a.iterator()
        );
    }

    @Test
    public void testMergeDisjoint() {
        List<Tuple> a = asList(
                t(0),
                t(3)
        );
        List<Tuple> b = asList(
                t(1),
                t(4)
        );
        List<Tuple> c = asList(
                t(2),
                t(5)
        );
        assertContents(
                merge(  asList(a.iterator(), b.iterator(), c.iterator())),
                t(0),
                t(1),
                t(2),
                t(3),
                t(4),
                t(5)
        );
    }

    @Test
    public void testOverlap() {
        List<Tuple> a = asList(
                t(0),
                t(1)
        );
        List<Tuple> b = asList(
                t(1),
                t(3)
        );
        List<Tuple> c = asList(
                t(1),
                t(4)
        );
        assertContents(
                merge(  asList(a.iterator(), b.iterator(), c.iterator())),
                t(0),
                t(1),
                t(3),
                t(4)
        );
    }

    private static void assertContents(Iterator<Tuple> test, Tuple ... check) {
        assertContents(test, asList(check).iterator());
    }

    private static void assertContents(Iterator<Tuple> test, Iterator<Tuple> check) {
        while (check.hasNext()) {
            assertTrue(test.hasNext(), "Iterator exhausted before expected end of Tuples");
            assertEquals(check.next(), test.next());
        }
        assertFalse(test.hasNext(), "Iterator not exhausted, but end of expected Tuples reached");
    }

    private static Tuple t(int b) {
        return new Tuple(new byte[][] { new byte[] { (byte) b } });
    }

}