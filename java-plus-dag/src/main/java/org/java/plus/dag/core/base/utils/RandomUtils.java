package org.java.plus.dag.core.base.utils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class RandomUtils {

    private RandomUtils() {
    }

    public static int nextInt() {
        return isOffRandom() ? 0 : ThreadLocalRandom.current().nextInt();
    }

    public static int nextInt(int bound) {
        return isOffRandom() ? 0 : ThreadLocalRandom.current().nextInt(bound);
    }

    public static int nextInt(int origin, int bound) {
        return isOffRandom() ? 0 : ThreadLocalRandom.current().nextInt(origin, bound);
    }

    public static long nextLong() {
        return isOffRandom() ? 0L : ThreadLocalRandom.current().nextLong();
    }

    public static long nextLong(long bound) {
        return isOffRandom() ? 0L : ThreadLocalRandom.current().nextLong();
    }

    public static long nextLong(long origin, long bound) {
        return isOffRandom() ? 0L : ThreadLocalRandom.current().nextLong(origin, bound);
    }

    public static double nextDouble() {
        return isOffRandom() ? 0D : ThreadLocalRandom.current().nextDouble();
    }

    public static double nextDouble(double bound) {
        return isOffRandom() ? 0D : ThreadLocalRandom.current().nextDouble(bound);
    }

    public static double nextDouble(double origin, double bound) {
        return isOffRandom() ? 0D : ThreadLocalRandom.current().nextDouble(origin, bound);
    }

    public static boolean nextBoolean() {
        return !isOffRandom() && ThreadLocalRandom.current().nextBoolean();
    }

    public static float nextFloat() {
        return isOffRandom() ? 0f : ThreadLocalRandom.current().nextFloat();
    }

    public static double nextGaussian() {
        return isOffRandom() ? 0D : ThreadLocalRandom.current().nextGaussian();
    }

    public static IntStream ints(long streamSize) {
        return isOffRandom() ?
                ThreadLocalRandom.current().ints(streamSize, 0, 1)
                : ThreadLocalRandom.current().ints(streamSize);
    }


    public static IntStream ints() {
        return isOffRandom() ?
                ThreadLocalRandom.current().ints(Long.MAX_VALUE, 0, 1)
                : ThreadLocalRandom.current().ints();
    }


    public static IntStream ints(long streamSize, int randomNumberOrigin, int randomNumberBound) {
        return isOffRandom() ?
                ThreadLocalRandom.current().ints(streamSize, 0, 1)
                : ThreadLocalRandom.current().ints(streamSize, randomNumberOrigin, randomNumberBound);
    }


    public static IntStream ints(int randomNumberOrigin, int randomNumberBound) {
        return isOffRandom() ?
                ThreadLocalRandom.current().ints(Long.MAX_VALUE, 0, 1)
                : ThreadLocalRandom.current().ints(randomNumberOrigin, randomNumberBound);
    }


    public static LongStream longs(long streamSize) {
        return isOffRandom() ?
                ThreadLocalRandom.current().longs(Long.MAX_VALUE, 0, 1)
                : ThreadLocalRandom.current().longs(streamSize);
    }


    public static LongStream longs() {
        return isOffRandom() ?
                ThreadLocalRandom.current().longs(Long.MAX_VALUE, 0, 1)
                : ThreadLocalRandom.current().longs();
    }


    public static LongStream longs(long streamSize, long randomNumberOrigin, long randomNumberBound) {
        return isOffRandom() ?
                ThreadLocalRandom.current().longs(streamSize, 0, 1)
                : ThreadLocalRandom.current().longs(streamSize, randomNumberOrigin, randomNumberBound);
    }


    public static LongStream longs(long randomNumberOrigin, long randomNumberBound) {
        return isOffRandom() ?
                ThreadLocalRandom.current().longs(Long.MAX_VALUE, 0, 1)
                : ThreadLocalRandom.current().longs(randomNumberOrigin, randomNumberBound);
    }


    public static DoubleStream doubles(long streamSize) {
        return isOffRandom() ?
                DoubleStream.generate(() -> 0D).limit(streamSize) : ThreadLocalRandom.current().doubles(streamSize);
    }


    public static DoubleStream doubles() {
        return isOffRandom() ?
                DoubleStream.generate(() -> 0D).limit(Long.MAX_VALUE) : ThreadLocalRandom.current().doubles();
    }


    public static DoubleStream doubles(long streamSize, double randomNumberOrigin, double randomNumberBound) {
        return isOffRandom() ?
                DoubleStream.generate(() -> 0D).limit(streamSize) : ThreadLocalRandom.current()
                                                                                     .doubles(streamSize, randomNumberOrigin, randomNumberBound);
    }


    public static DoubleStream doubles(double randomNumberOrigin, double randomNumberBound) {
        return isOffRandom() ?
                DoubleStream.generate(() -> 0D).limit(Long.MAX_VALUE) : ThreadLocalRandom.current()
                                                                                         .doubles(randomNumberOrigin, randomNumberBound);
    }


    public static void nextBytes(byte[] bytes) {
        ThreadLocalRandom.current().nextBytes(bytes);
    }

    public static boolean isOffRandom() {
        return ThreadLocalUtils.isOffRandom();
    }
}
