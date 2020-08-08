package org.java.plus.dag.core.operator.join;

import java.util.function.BiPredicate;

/**
 * Stream join key match predicate
 * @author seven.wxy
 * @date 2018/9/5
 */
public class MatchPredicate {
    static final BiPredicate<Object, Object> EQUALS = Object::equals;

    public static <KL, KR> BiPredicate<KL, KR> equals() {
        return (BiPredicate<KL, KR>)EQUALS;
    }
}