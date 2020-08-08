package org.java.plus.dag.core.operator.join;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Make Supplier operator lazy
 * @author seven.wxy
 * @date 2018/11/16
 */
public class LazySupplier<T> implements Supplier<T> {
    private transient Supplier<T> supplier;
    private volatile T value;

    public LazySupplier(Supplier<T> supplier) {
        this.supplier = Objects.requireNonNull(supplier);
    }

    @Override
    public T get() {
        if (value == null) {
            synchronized (this) {
                if (value == null) {
                    value = Objects.requireNonNull(supplier.get());
                    supplier = null;
                }
            }
        }
        return value;
    }

    /**
     * create a lazy supplier
     * @param supplier the supplier need to lazy process
     * @param <T>
     * @return lazy supplier
     */
    public static <T> LazySupplier<T> of(Supplier<T> supplier) {
        return new LazySupplier<>(supplier);
    }
}
