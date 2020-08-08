package org.java.plus.dag.core.base.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.function.Function;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author seven.wxy
 * @date 2019/6/21
 */
public class CustomInPredicate<T, K> implements com.google.common.base.Predicate<T>, Serializable {
    private final Collection<?> target;
    private final Function<T, K> keyFunction;

    public CustomInPredicate(Collection<?> target, Function<T, K> keyFunction) {
        this.target = checkNotNull(target);
        this.keyFunction = checkNotNull(keyFunction);
    }

    @Override
    public boolean apply(@Nullable T t) {
        try {
            return target.contains(keyFunction.apply(t));
        } catch (NullPointerException e) {
            return false;
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj instanceof CustomInPredicate) {
            CustomInPredicate<T, K> that = (CustomInPredicate<T, K>)obj;
            return target.equals(that.target);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return target.hashCode();
    }

    @Override
    public String toString() {
        return "In(" + target + ")";
    }

    private static final long serialVersionUID = 0;
}
