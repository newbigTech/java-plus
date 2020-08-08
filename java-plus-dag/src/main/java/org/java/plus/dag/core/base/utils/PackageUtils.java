package org.java.plus.dag.core.base.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author youku
 */
public class PackageUtils {

    public static Map<String, Class<?>> findClassesInPackage(ClassFilter filter, String... packageName) {
        ClassLoader classLoader = PackageUtils.class.getClassLoader();
        Map<String, Class<?>> classes = Maps.newHashMap();
        Map<Pair<String, URL>, String[]> rt = Arrays.stream(packageName).collect(Collectors.groupingBy(p -> {
            String packageDirName = p.replace('.', '/');
            URL url = null;
            try {
                url = classLoader.getResource(packageDirName);
            } catch (Exception ignore) {
            }
            return new Pair<>(url == null ? null : StringUtils.substringBefore(url.getPath(), "!"), url);
        }, Collectors.collectingAndThen(Collectors.toList(),
            list -> list.stream().map(s -> StringUtils.replaceChars(s, '.', '/')).collect(Collectors.toList()).toArray(new String[0]))));
        rt.forEach((k, v) -> {
            URL url = k.right;
            if (Objects.nonNull(url)) {
                String protocol = url.getProtocol();
                if ("file".equals(protocol)) {
                    Arrays.stream(v).parallel().flatMap(p -> {
                        try {
                            return findClassesInDirPackage(StringUtils.replaceChars(p, '/', '.'),
                                URLDecoder.decode(url.getFile(), "UTF-8"), Lists.newArrayList()).stream();
                        } catch (UnsupportedEncodingException ignore) {
                        }
                        return Stream.empty();
                    }).collect(Collectors.toList()).forEach(e -> {
                        Class<?> cls = genClass(StringUtils.EMPTY, e, filter);
                        if (cls != null) {
                            classes.put(StringUtils.substringAfterLast(e, "."), cls);
                        }
                    });
                } else if ("jar".equals(protocol)) {
                    try (JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile()) {
                        if (Objects.nonNull(jar)) {
                            jar.stream().parallel()
                                .filter(entry -> !entry.isDirectory() && StringUtils.endsWith(entry.getName(), ".class")
                                    && StringUtils.startsWithAny(entry.getName(), v))
                                .map(entry -> {
                                    String name = entry.getName();
                                    if (name.charAt(0) == '/') {
                                        name = name.substring(1);
                                    }
                                    return StringUtils.replaceChars(name.substring(0, name.length() - 6), '/', '.');
                                }).collect(Collectors.toList()).forEach(s -> {
                                Class<?> cls = genClass(StringUtils.EMPTY, s, filter);
                                if (cls != null) {
                                    classes.put(StringUtils.substringAfterLast(s, "."), cls);
                                }
                            });
                        }
                    } catch (Exception exp) {
                        // ignore
                    }
                }
            }
        });
        return classes;
    }

    private static Class<?> genClass(String packageName, String clazzName, ClassFilter filter) {
        String clazz = makeFullClassName(packageName, clazzName);
        Class<?> cls = null;
        if (filter == null || filter.preFilter(clazz)) {
            try {
                cls = Class.forName(clazz);
            } catch (Throwable ignore) {
            }
        }
        return cls != null && Modifier.isPublic(cls.getModifiers()) && (filter == null || filter.filter(cls)) ? cls : null;
    }

    private static List<String> findClassesInDirPackage(String packageName, String packagePath, List<String> classes) {
        File dir = new File(packagePath);
        if (dir.exists() && dir.isDirectory()) {
            File[] dirFiles = dir.listFiles(file -> file.isDirectory() || (file.getName().endsWith(".class")));
            if (dirFiles != null) {
                for (File file : dirFiles) {
                    if (file.isDirectory()) {
                        findClassesInDirPackage(makeFullClassName(packageName, file.getName()), file.getAbsolutePath(), classes);
                    } else {
                        String name = file.getName();
                        classes.add(packageName + "." + name.substring(0, name.length() - 6));
                    }
                }
            }
        }
        return classes;
    }

    private static String makeFullClassName(String pkg, String cls) {
        return pkg.length() > 0 ? pkg + "." + cls : cls;
    }

    /**
     *
     */
    public interface ClassFilter {

        /**
         * ¹ýÂË
         *
         * @param className
         * @return
         */
        boolean preFilter(String className);

        /**
         * ¹ýÂË
         *
         * @param clazz
         * @return
         */
        boolean filter(Class clazz);
    }

    @AllArgsConstructor
    private static final class Pair<A, B> {
        private A left;
        private B right;

        @Override
        public int hashCode() {
            return Objects.hashCode(left);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Pair && Objects.equals(left, ((Pair) obj).left);
        }
    }
}
