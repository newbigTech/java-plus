package org.java.plus.dag.core.base.utils;

import java.io.CharArrayWriter;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * @author youku
 */
@SuppressWarnings("unchecked")
public class StrUtils {
    private static BitSet dontNeedEncoding = new BitSet(256);
    private static final int CASE_DIFF = ('a' - 'A');

    static {
        int i;
        for (i = 'a'; i <= 'z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = 'A'; i <= 'Z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = '0'; i <= '9'; i++) {
            dontNeedEncoding.set(i);
        }
        dontNeedEncoding.set(' ');
        dontNeedEncoding.set('-');
        dontNeedEncoding.set('_');
        dontNeedEncoding.set('.');
        dontNeedEncoding.set('*');
    }

    public static String encode(String s, Charset charset) {
        boolean needToChange = false;
        StringBuilder out = new StringBuilder(s.length());
        CharArrayWriter charArrayWriter = new CharArrayWriter();
        for (int i = 0; i < s.length(); ) {
            int c = s.charAt(i);
            if (dontNeedEncoding.get(c)) {
                if (c == ' ') {
                    c = '+';
                    needToChange = true;
                }
                out.append((char)c);
                i++;
            } else {
                do {
                    charArrayWriter.write(c);
                    if (c >= 0xD800 && c <= 0xDBFF) {
                        if ((i + 1) < s.length()) {
                            int d = s.charAt(i + 1);
                            if (d >= 0xDC00 && d <= 0xDFFF) {
                                charArrayWriter.write(d);
                                i++;
                            }
                        }
                    }
                    i++;
                } while (i < s.length() && !dontNeedEncoding.get((c = s.charAt(i))));
                charArrayWriter.flush();
                String str = new String(charArrayWriter.toCharArray());
                byte[] ba = str.getBytes(charset);
                for (byte b : ba) {
                    out.append('%');
                    char ch = Character.forDigit((b >> 4) & 0xF, 16);
                    if (Character.isLetter(ch)) {
                        ch -= CASE_DIFF;
                    }
                    out.append(ch);
                    ch = Character.forDigit(b & 0xF, 16);
                    if (Character.isLetter(ch)) {
                        ch -= CASE_DIFF;
                    }
                    out.append(ch);
                }
                charArrayWriter.reset();
                needToChange = true;
            }
        }
        return (needToChange ? out.toString() : s);
    }

    public static String decode(String s, Charset enc) {
        boolean needToChange = false;
        int numChars = s.length();
        StringBuilder sb = new StringBuilder(numChars > 500 ? numChars / 2 : numChars);
        int i = 0;
        char c;
        byte[] bytes = null;
        while (i < numChars) {
            c = s.charAt(i);
            switch (c) {
                case '+':
                    sb.append(' ');
                    i++;
                    needToChange = true;
                    break;
                case '%':
                    try {
                        if (bytes == null) {
                            bytes = new byte[(numChars - i) / 3];
                        }
                        int pos = 0;
                        while (((i + 2) < numChars) &&
                            (c == '%')) {
                            int v = Integer.parseInt(s.substring(i + 1, i + 3), 16);
                            if (v < 0) {
                                throw new IllegalArgumentException(
                                    "URLDecoder: Illegal hex characters in escape (%) pattern - negative value");
                            }
                            bytes[pos++] = (byte)v;
                            i += 3;
                            if (i < numChars) {
                                c = s.charAt(i);
                            }
                        }
                        if ((i < numChars) && (c == '%')) {
                            throw new IllegalArgumentException(
                                "URLDecoder: Incomplete trailing escape (%) pattern");
                        }
                        sb.append(new String(bytes, 0, pos, enc));
                    } catch (Exception e) {
                        return s;
                    }
                    needToChange = true;
                    break;
                default:
                    sb.append(c);
                    i++;
                    break;
            }
        }
        return (needToChange ? sb.toString() : s);
    }

    public static String objectToStr(Object obj, String join) {
        return obj instanceof List ?
            (String)ListUtils.emptyIfNull((List)obj).stream().filter(Objects::nonNull).map(Object::toString).collect(
                Collectors.joining(join))
            : StrUtils.trimToEmpty(obj);
    }

    public static Integer strToInteger(String str) {
        try {
            return Integer.valueOf(str);
        } catch (Exception ignore) {

        }
        return null;
    }

    public static String listToStr(List<?> list) {
        return objectToStr(list, StringUtils.EMPTY);
    }

    public static List<String> strToList(String value, String sep) {
        return StringUtils.isBlank(value) ? Collections.emptyList() : Lists.newArrayList(
            StringUtils.splitByWholeSeparator(value, sep));
    }

    public static Map<String, String> strToMap(String str, String sep1, String sep2) {
        return strToMap(str, sep1, sep2, false);
    }

    public static Set<String> strToStrSet(String value, String sep) {
        return strToSet(value, sep, Function.identity());
    }

    public static <T> Set<T> strToSet(String value, String sep, Function<String, T> func) {
        return StringUtils.isEmpty(value = StringUtils.trimToEmpty(value)) ? Collections.emptySet() :
            Arrays.stream(StringUtils.splitByWholeSeparator(value, sep)).map(func).filter(Objects::nonNull).collect(
                Collectors.toSet());
    }

    public static List<String> strToStrList(String value, String sep) {
        return strToList(value, sep, Function.identity());
    }

    public static <T> List<T> strToList(String value, String sep, Function<String, T> func) {
        return StringUtils.isEmpty(value = StringUtils.trimToEmpty(value)) ? Collections.emptyList() :
            Arrays.stream(StringUtils.splitByWholeSeparator(value, sep)).map(func).filter(Objects::nonNull).collect(
                Collectors.toList());
    }

    public static Map<String, String> strToMap(String str, String sep1, String sep2, boolean kvInvert) {
        return strToMap(str, sep1, sep2, kvInvert, null);
    }

    public static Map<String, String> strToMap(String str, String sep1, String sep2, boolean kvinvert,
                                               Boolean oneValueForKey) {
        if (StringUtils.isEmpty(str)) {
            return Collections.emptyMap();
        }
        Map<String, String> hmParam = Maps.newLinkedHashMap();
        String[] paramPairs = StringUtils.splitByWholeSeparator(str, sep1);
        for (String pair : paramPairs) {
            String[] subPair = StringUtils.splitByWholeSeparator(StringUtils.trimToEmpty(pair), sep2);
            if (oneValueForKey != null && subPair.length == 1) {
                if (oneValueForKey) {
                    hmParam.put(subPair[0], null);
                } else {
                    hmParam.put(null, subPair[0]);
                }
            } else if (subPair.length > 1) {
                if (kvinvert) {
                    hmParam.put(subPair[1], subPair[0]);
                } else {
                    hmParam.put(subPair[0], subPair[1]);
                }
            }
        }
        return hmParam;
    }

    public static String getOrDefault(String str, String defaultValue) {
        if (StringUtils.isNotBlank(str)) {
            return str;
        }
        return defaultValue;
    }

    public static String map2Str(Map<String, String> map, String sep1, String sep2) {
        StringBuilder strSb = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            strSb.append(entry.getKey()).append(sep1).append(entry.getValue());
            strSb.append(sep2);
        }
        return strSb.toString();
    }

    public static <T> String toString(Object obj, T t) {
        if (obj == null) {
            return t == null ? null : t.toString();
        } else {
            return obj.toString();
        }
    }

    public static <T> String trimToString(Object obj, T t) {
        if (obj == null) {
            return t == null ? null : t.toString().trim();
        } else {
            return obj.toString().trim();
        }
    }

    public static <T> String trimToString(T t) {
        String str = toString(t, null);
        return str == null ? null : str.trim();
    }

    public static <T> String trimToEmpty(T t) {
        return toString(t, StringUtils.EMPTY).trim();
    }

    public static Double objToDouble(Object obj) {
        return obj == null || obj instanceof Double ? (Double)obj : NumberUtils.toDouble(obj.toString());
    }

    public static <T> boolean isNotBlank(T t) {
        if (t == null) {
            return false;
        } else {
            return StringUtils.isNotBlank(t.toString());
        }
    }

    public static <T> String trimToDefault(String defaultValue, Predicate<String> predicate, T... values) {
        return Optional.ofNullable(values).map(Stream::of).orElseGet(Stream::empty)
            .map(StrUtils::trimToString).filter(predicate).findFirst().orElse(String.valueOf(defaultValue));
    }

    public static <T> String findNotNull(String defaultValue, T... values) {
        return trimToDefault(defaultValue, Objects::nonNull, values);
    }

    public static <T> String findNotBlank(T... values) {
        return trimToDefault(StringUtils.EMPTY, StringUtils::isNotEmpty, values);
    }

    public static boolean isEmpty(Object obj) {
        return obj == null || obj.toString().length() == 0;
    }

    public static Object get(Object array, int index) {
        try {
            return Array.get(array, index);
        } catch (Exception ignore) {
        }
        return null;
    }

    public static List<String> split(final String str, final char separatorChar) {
        return splitWorker(str, separatorChar, false, -1, true);
    }

    public static List<String> split(final String str, final char separatorChar, int max) {
        return splitWorker(str, separatorChar, false, max, true);
    }

    public static List<String> splitPreserveAllTokens(final String str, final char separatorChar) {
        return splitWorker(str, separatorChar, true, -1, true);
    }

    private static List<String> splitWorker(final String str, final char separatorChar, final boolean preserveAllTokens,
                                            int max, final boolean fromStart) {
        if (str == null) {
            return null;
        }
        final int len = str.length();
        if (len == 0) {
            return Collections.emptyList();
        }
        final List<String> list = Lists.newArrayList();
        boolean match = false, lastMatch = true;
        if (fromStart) {
            int i = 0, start = 0, count = 1;
            while (i < len) {
                if (str.charAt(i) == separatorChar) {
                    if (match) {
                        list.add(str.substring(start, i));
                    } else if (preserveAllTokens) {
                        list.add(StringUtils.EMPTY);
                    }
                    match = false;
                    if (count++ == max) {
                        lastMatch = false;
                        break;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
            if (match || preserveAllTokens && lastMatch) {
                list.add(str.substring(start, i));
            }
        } else {
            int i = len - 1, start = len, count = 1;
            while (i > 0) {
                if (str.charAt(i) == separatorChar) {
                    if (match) {
                        list.add(str.substring(i + 1, start));
                    } else if (preserveAllTokens) {
                        list.add(StringUtils.EMPTY);
                    }
                    match = false;
                    if (count++ == max) {
                        lastMatch = false;
                        break;
                    }
                    start = i--;
                    continue;
                }
                i--;
                match = true;
            }
            if (match || preserveAllTokens && lastMatch) {
                list.add(str.substring(i, start));
            }
        }
        return list;
    }

    public static boolean startWith(String str, String prefix, int beginIndex) {
        if (str == null || prefix == null) {
            return (str == null && prefix == null);
        }
        if (prefix.length() > str.length() - beginIndex) {
            return false;
        }
        return str.regionMatches(beginIndex, prefix, 0, prefix.length());
    }

    public static boolean startWith(String str, String prefix, int beginIndex, int endIndex) {
        if (str == null || prefix == null) {
            return (str == null && prefix == null);
        }
        if (prefix.length() > endIndex - beginIndex) {
            return false;
        }
        return str.regionMatches(beginIndex, prefix, 0, prefix.length());
    }
}