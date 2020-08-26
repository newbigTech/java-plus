package org.java.plus.dag.util;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.TypeUtils; 

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
//import org.java.plus.dag.core.base.utils.PackageUtils;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.testng.internal.PackageUtils;
 

/**
 * @author seven.wxy
 * @date 2019/1/29
 */
public class GenerateDoc {
//    private static final String PACKAGE = "com.taobao.recommendplatform.solutions.ykrcmd.common.";
    private static final String PACKAGE = "org.java.plus.dag.";
    private static final Set<String> EXCLUDES =
        Sets.newHashSet(PACKAGE + "service.AbstractBaseProcessor", PACKAGE + "service.AbstractProcessor");

    public void generateConfigDocByClasspath(String classPath) throws Exception {
        String configKey = StringUtils.replace(classPath, PACKAGE, StringUtils.EMPTY).replace(StringPool.DOT,
            StringPool.SLASH);
        System.out.println("* " + configKey);
        System.out.println();
        System.out.println("| **配置项名称** | **默认值** | **说明** |");
        System.out.println("| :--- | :--- | :--- |");
        Class clazz = Class.forName(classPath);
        Object processor = getBeanByDefaultConfig(configKey, clazz);
        for (Field field : FieldUtils.getAllFieldsList(clazz)) {
            String declaringClass = field.getDeclaringClass().getName();
            if (!EXCLUDES.contains(declaringClass)) {
                ConfigInit reflectInit = field.getAnnotation(ConfigInit.class);
                if (reflectInit != null) {
                    Object defaultValue = FieldUtils.readField(processor, field.getName(), true);
                    if (!Objects.equals(reflectInit.defaultValue(), ConstantsFrame.DEFAULT_CONFIG_VALUE)) {
                        defaultValue = TypeUtils.cast(reflectInit.defaultValue(), field.getGenericType(),ParserConfig.getGlobalInstance());
                    }
                    String fieldName = StringUtils.isEmpty(reflectInit.name()) ? field.getName() : reflectInit.name();
                    String desc = reflectInit.desc();
                    System.out.println("| " + fieldName + " | " + defaultValue + " | " + desc + " | ");
                }
            }
        }
    }
 
    public void generateConfigDoc() throws Exception {
//        generateConfigDocByClasspath(PACKAGE + "service.other.RowExpandProcessor");
        generateConfigDocByClasspath(PACKAGE + "demo.other.DemoProcessor");
        }
 
    public void generateConfigDocAll() throws Exception {
        List<String> classes = getClassesInPackage(PACKAGE + "engine");
        classes.addAll(getClassesInPackage(PACKAGE + "service.recall"));
        classes.addAll(getClassesInPackage(PACKAGE + "service.filter"));
        classes.addAll(getClassesInPackage(PACKAGE + "service.rank"));
        classes.addAll(getClassesInPackage(PACKAGE + "service.rerank"));
        classes.addAll(getClassesInPackage(PACKAGE + "service.merge"));
        classes.addAll(getClassesInPackage(PACKAGE + "service.adaptor"));
        classes.addAll(getClassesInPackage(PACKAGE + "service.strategy"));
        classes.addAll(getClassesInPackage(PACKAGE + "service.other"));
        for (String clz : classes) {
            generateConfigDocByClasspath(clz);
        }
    }

    public List<String> getClassesInPackage(String packageName) throws Exception{
        List<String> classes = Lists.newArrayList(PackageUtils.findClassesInPackage(packageName, Lists.newArrayList(), Lists.newArrayList()));
        return classes.stream()
            .filter(p -> !StringUtils.contains(p, "$")
                && !StringUtils.endsWith(p, "Test")
                && !StringUtils.contains(p, "recall.MockRecall")
                && !StringUtils.contains(p, "engine.Schedule")
                && !StringUtils.contains(p, "filter.ItemInfo")
                && !StringUtils.contains(p, ".Base")
                && !StringUtils.contains(p, ".Demo")
                && !StringUtils.contains(p, "filter.Blacklist")
                && !StringUtils.contains(p, "filter.FeedbackItem")
                && !StringUtils.contains(p, "other.FeedbackReason")
            )
            .collect(Collectors.toList());
    }

    
    public <T> T getBeanByDefaultConfig(String configKey, Class<T> clazz) {
        return TppObjectFactory.getBean(configKey, StringUtils.EMPTY, ConstantsFrame.NULL_JSON_OBJECT, clazz);
    }

    public <T> T getBean(String configKey, JSONObject configValue, Class<T> clazz) {
        return TppObjectFactory.getBean(configKey, StringUtils.EMPTY, configValue, clazz);
    }
}
