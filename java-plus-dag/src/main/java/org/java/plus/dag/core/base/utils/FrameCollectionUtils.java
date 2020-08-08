package org.java.plus.dag.core.base.utils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.java.plus.dag.core.base.utils.RandomUtils.isOffRandom;

public class FrameCollectionUtils {
    private FrameCollectionUtils() {
    }

    public static void shuffle(List<?> list, Random rnd) {
        if (!isOffRandom()) {
            Collections.shuffle(list, rnd);
        }
    }

    public static void shuffle(List<?> list) {
        if (!isOffRandom()) {
            Collections.shuffle(list);
        }
    }

    @SuppressWarnings("unchecked")
    public static List<?> shuffle(List<?> list, int count) {
        int size = Objects.isNull(list) ? 0 : list.size();
        if (count > 0 && count < size) {
            if (!isOffRandom()) {
                int index = ThreadLocalRandom.current().nextInt(size);
                int toAdd = count - size + index;
                List result = new ArrayList<>(count);
                for (int i = index, end = toAdd > 0 ? size : index + count; i < end; i++) {
                    result.add(list.get(i));
                }
                for (int i = 0; i < toAdd; i++) {
                    result.add(list.get(i));
                }
                return result;
            }
            return list.subList(0, count);
        }
        return Lists.newArrayList(list);
    }
}
