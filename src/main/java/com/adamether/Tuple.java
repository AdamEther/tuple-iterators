package com.adamether;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A tuple of byte-encoded values
 */
public class Tuple {
    private final byte[][] values;

    public Tuple(byte[] ... values) {
        this.values = values;
    }

    public Tuple(List<byte[]> values) {
        this(values.toArray(new byte[0][]));
    }

    /**
     * @return the number of distinct byte-encoded values that make up this Tuple
     */
    public int count() {
        return values.length;
    }

    /**
     * @param i the index within the key you wish to retrieve the value for
     * @return the i'th byte-encoded value of the key
     */
    public byte[] get(int i) {
        return values[i];
    }

    public boolean equals(Object that) {
        return that instanceof Tuple && equals((Tuple) that);
    }

    public boolean equals(Tuple that) {
        if (this == that || that == null) {
            return this == that;
        }
        if (this.values.length != that.values.length) {
            return false;
        }
        return IntStream.range(0, this.values.length)
                .allMatch(i -> Arrays.equals(this.values[i], that.values[i]));
    }

    /**
     * Compare each component of two keys, returning the first non-zero result
     */
    public static int compare(Tuple left, Tuple right) {
        assert left.count() == right.count();
        return IntStream.range(0, left.count())
                .map(i -> compareComponent(left.get(i), right.get(i)))
                .filter(c -> c != 0)
                .findFirst()
                .orElse(0);
    }

    /**
     * Compare a single pair of key components.
     * This is a straight forward binary comparison, returning the difference between the first mismatching byte,
     * with otherwise matching arrays sorting the shorter array first.
     */
    public static int compareComponent(byte[] left, byte[] right) {
        int len = Math.min(left.length, right.length);
        for (int i = 0 ; i < len ; ++i) {
            int c = left[i] - right[i];
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
