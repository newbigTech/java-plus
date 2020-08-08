package org.java.plus.dag.core.base.em;

import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.NumberUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * @author seven.wxy
 * @date 2019/7/17
 */
public interface FieldNameEnum extends Serializable, Cloneable, EnumInterface {
    /**
     * Get field type
     *
     * @return
     */
    FieldType getFieldType();

    /**
     * Set field type
     *
     * @param fieldType
     */
    void setFieldType(FieldType fieldType);

    /**
     * Get field class
     *
     * @return
     */
    Class<?> getClazz();

    /**
     * Set field class
     *
     * @param clazz
     */
    void setClazz(Class<?> clazz);

    /**
     * Transfer field value
     *
     * @param value
     * @return
     */
    default Object transfer(final Object value) {
        return Objects.isNull(value) ? null : getFieldType().transfer(value, getClazz());
    }

    /**
     * Init field class and fieldType, all subclass must call this method in constructor
     *
     * @param clazz
     */
    default void initClassAndFieldType(Class<?> clazz) {
        setClazz(clazz);
        FieldType fieldType;
        if (clazz == long.class || clazz == Long.class) {
            fieldType = FieldType.Long;
        } else if (clazz == String.class) {
            fieldType = FieldType.String;
        } else if (clazz == double.class || clazz == Double.class) {
            fieldType = FieldType.Double;
        } else if (clazz == int.class || clazz == Integer.class) {
            fieldType = FieldType.Integer;
        } else if (List.class.isAssignableFrom(clazz)) {
            fieldType = FieldType.List;
        } else if (Map.class.isAssignableFrom(clazz)) {
            fieldType = FieldType.Map;
        } else if (Set.class.isAssignableFrom(clazz)) {
            fieldType = FieldType.Set;
        } else if (clazz == boolean.class || clazz == Boolean.class) {
            fieldType = FieldType.Boolean;
        } else if (clazz.isEnum()) {
            fieldType = FieldType.Enum;
        } else if (clazz == byte.class || clazz == Byte.class) {
            fieldType = FieldType.Byte;
        } else if (clazz == short.class || clazz == Short.class) {
            fieldType = FieldType.Short;
        } else if (clazz == char.class || clazz == Character.class) {
            fieldType = FieldType.Character;
        } else if (clazz == float.class || clazz == Float.class) {
            fieldType = FieldType.Float;
        } else if (clazz.isArray()) {
            fieldType = FieldType.Array;
        } else if (Queue.class.isAssignableFrom(clazz)) {
            fieldType = FieldType.Queue;
        } else if (Number.class.isAssignableFrom(clazz)) {
            fieldType = FieldType.Number;
        } else if (CharSequence.class.isAssignableFrom(clazz)) {
            fieldType = FieldType.CharSequence;
        } else {
            fieldType = FieldType.other;
        }
        setFieldType(fieldType);
    }

    enum FieldType {
        /**
         *
         */
        Integer {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz != int.class && clazz != java.lang.Integer.class) {
                    String stringValue = value.toString();
                    if (StringUtils.isNotEmpty(stringValue)) {
                        return java.lang.Integer.parseInt(stringValue);
                    } else {
                        return null;
                    }
                }
                return value;
            }
        },
        Byte {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz != byte.class && clazz != java.lang.Byte.class) {
                    String stringValue = value.toString();
                    if (StringUtils.isNotEmpty(stringValue)) {
                        return java.lang.Byte.parseByte(stringValue);
                    } else {
                        return null;
                    }
                }
                return value;
            }
        },
        Character {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz != char.class && clazz != java.lang.Character.class) {
                    String str = value.toString();
                    if (str.length() == 1) {
                        return str.charAt(0);
                    }
                    throw new IllegalArgumentException(value + " parameter type should be char or Character type");
                }
                return value;
            }
        },
        Short {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz != short.class && clazz != java.lang.Short.class) {
                    String stringValue = value.toString();
                    if (StringUtils.isNotEmpty(stringValue)) {
                        return java.lang.Short.valueOf(stringValue);
                    } else {
                        return null;
                    }
                }
                return value;
            }
        },
        Long {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz != long.class && clazz != java.lang.Long.class) {
                    String stringValue = value.toString();
                    if (StringUtils.isNotEmpty(stringValue)) {
                        return java.lang.Long.valueOf(stringValue);
                    } else {
                        return null;
                    }
                }
                return value;
            }
        },
        Float {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz != float.class && clazz != java.lang.Float.class) {
                    String stringValue = value.toString();
                    if (StringUtils.isNotEmpty(stringValue)) {
                        return java.lang.Float.valueOf(stringValue);
                    } else {
                        return null;
                    }
                }
                return value;
            }
        },
        Double {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz != double.class && clazz != java.lang.Double.class) {
                    String stringValue = value.toString();
                    if (StringUtils.isNotEmpty(stringValue)) {
                        return java.lang.Double.valueOf(stringValue);
                    } else {
                        return null;
                    }
                }
                return value;
            }
        },
        String {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                return value.toString();
            }
        },
        Boolean {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                Class<?> clazz = value.getClass();
                if (clazz == boolean.class || clazz == java.lang.Boolean.class) {
                    return value;
                } else {
                    String str = value.toString();
                    if ("true".equalsIgnoreCase(str)) {
                        return java.lang.Boolean.TRUE;
                    } else if ("false".equalsIgnoreCase(str)) {
                        return java.lang.Boolean.FALSE;
                    }
                    throw new IllegalArgumentException(value + " parameter type should be boolean or Boolean type");
                }
            }
        },
        Number {
            @SuppressWarnings({"unchecked"})
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!java.lang.Number.class.isAssignableFrom(value.getClass())) {
                    return NumberUtils.parseNumber(value.toString(), (Class) fieldClazz);
                }
                return value;
            }
        },
        Set {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!java.util.Set.class.isAssignableFrom(value.getClass())) {
                    throw new IllegalArgumentException(value + " parameter type should implement Set interface");
                }
                return value;
            }
        },
        List {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!java.util.List.class.isAssignableFrom(value.getClass())) {
                    throw new IllegalArgumentException(value + " parameter type should implement List interface");
                }
                return value;
            }
        },
        Map {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!java.util.Map.class.isAssignableFrom(value.getClass())) {
                    throw new IllegalArgumentException(value + " parameter type should implement Map interface");
                }
                return value;
            }
        },
        Queue {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!java.util.Queue.class.isAssignableFrom(value.getClass())) {
                    throw new IllegalArgumentException(value + " parameter type should implement Queue interface");
                }
                return value;
            }
        },
        Array {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                java.lang.reflect.Array.getLength(value);
                return value;
            }
        },
        Enum {
            @Override
            @SuppressWarnings({"unchecked"})
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!value.getClass().isEnum()) {
                    return java.lang.Enum.valueOf((Class) fieldClazz, value.toString());
                }
                return value;
            }
        },
        CharSequence {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!java.lang.CharSequence.class.isAssignableFrom(value.getClass())) {
                    throw new IllegalArgumentException(value + " parameter type should extends CharSequence type");
                }
                return value;
            }
        },
        other {
            @Override
            public Object transfer(@NonNull final Object value, Class<?> fieldClazz) {
                if (!fieldClazz.isAssignableFrom(value.getClass())) {
                    throw new IllegalArgumentException(
                            value + " parameter type should extends " + fieldClazz.getName() + " type");
                }
                return value;
            }
        };

        abstract Object transfer(@NonNull final Object value, Class<?> fieldClazz);
    }
}
