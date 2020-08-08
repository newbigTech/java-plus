package org.java.plus.dag.core.base.utils;

/**
 * @author youku
 */
public class MathUtil {
    public static boolean moreThan(double d1, double d2) {
        return Double.compare(d1, d2) > 0;
    }

    public static boolean equal(double d1, double d2) {
        return Double.compare(d1, d2) == 0;
    }

    public static boolean lessThan(double d1, double d2) {
        return Double.compare(d1, d2) < 0;
    }

    public static boolean moreThan(Double d1, Double d2) {
        return d1 != null && (d2 == null || d1.compareTo(d2) > 0);
    }

    public static boolean lessThan(Double d1, Double d2) {
        return d2 != null && (d1 == null || d2.compareTo(d1) > 0);
    }

    public static boolean equal(Double d1, Double d2) {
        return (d1 == null && d2 == null) || (d1 != null && d2 != null && d1.compareTo(d2) == 0);
    }
}
