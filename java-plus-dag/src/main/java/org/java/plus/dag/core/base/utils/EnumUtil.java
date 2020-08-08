package org.java.plus.dag.core.base.utils;

import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.AllFieldName;
import lombok.Getter;
import org.apache.commons.lang3.EnumUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.java.plus.dag.core.base.constants.ConstantsFrame.PACKAGE_NAME;

/**
 * Enum op util
 *
 * @author youku
 */
@SuppressWarnings("all")
public class EnumUtil {
    @Getter
    private static final Map<String, Class<? extends Enum>> ENUM_CLASS_CACHE = Maps.newConcurrentMap();
    /**
     * Global Enum cache , should guarantee enum name unique , or else value is uncertain
     */
    @Getter
    private static final Map<String, Enum<? extends Enum>> ENUM_CACHE = Maps.newHashMap();

    private static final String[] DEFAULT_PACKAGE = {PACKAGE_NAME + "core.base.em", PACKAGE_NAME + "biz.base.em"};
    private static final AtomicBoolean INIT = new AtomicBoolean();

    public static void addClass(Class<? extends Enum> enumClass) {
        ENUM_CLASS_CACHE.put(enumClass.getSimpleName(), enumClass);
    }

    public static <T extends Enum<T>> T getEnumByDefault(String clazzName, String enumName) {
        init();
        T result = getEnum(clazzName, enumName);
        return result != null ? result : (T) getEnum(enumName);
    }

    public static <T extends Enum<T>> T getEnum(String classDotName) {
        init();
        List<String> list = StrUtils.split(classDotName, StringPool.C_DOT, 2);
        T rt;
        if (list.size() == 2) {
            String className = list.get(0);
            String enumName = list.get(1);
            rt = getEnum(className, enumName);
        } else {
            rt = (T) EnumUtils.getEnum(AllFieldName.class, classDotName);
        }
        return rt;
    }

    public static <T extends Enum<T>> T getEnumInAllEnum(String classDotName) {
        T t = getEnum(classDotName);
        return t == null ? (T) ENUM_CACHE.get(classDotName) : t;
    }

    public static <T extends Enum<T>> T getEnum(String clazzName, String enumName) {
        init();
        return (T) EnumUtils.getEnum(getEnumClass(clazzName), enumName);
    }

    private static Class getEnumClass(String clazzName) {
        init();
        return ENUM_CLASS_CACHE.get(clazzName);
    }

    private static void init() {
        if (!INIT.getAndSet(true)) {
            EnumInitClass.init();
        }
    }

    private static final class EnumInitClass {
        static {
            ENUM_CLASS_CACHE.putAll((Map) PackageUtils.findClassesInPackage(new PackageUtils.ClassFilter() {
                @Override
                public boolean preFilter(String className) {
                    return true;
                }

                @Override
                public boolean filter(Class clazz) {
                    return clazz != null && Enum.class.isAssignableFrom(clazz);
                }
            }, DEFAULT_PACKAGE));
            ENUM_CLASS_CACHE.values()
                .forEach(e -> {
                    for (Enum en : e.getEnumConstants()) {
                        ENUM_CACHE.put(en.name(), en);
                    }
                });
        }

        private static void init() {

        }
    }
}
