package org.java.plus.dag.core.base.utils;

import java.lang.reflect.Field;
import java.util.Objects;

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.TypeUtils;

import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * @author seven.wxy
 * @date 2019/9/6
 */
public class InitUtils {
    public static Object getFieldValue(Object instance, Field field, ConfigInit annotation, ProcessorConfig config) {
        Object value = null;
        try {
            Object defaultValue = FieldUtils.readField(instance, field.getName(), true);
            if (!Objects.equals(annotation.defaultValue(), ConstantsFrame.DEFAULT_CONFIG_VALUE)) {
                defaultValue = TypeUtils.cast(annotation.defaultValue(), field.getGenericType(),
                    ParserConfig.getGlobalInstance());
            }
            value = JsonUtils.getParam(config,
                StringUtils.isEmpty(annotation.name()) ? field.getName() : annotation.name(), defaultValue);
            // local env, change async to false for debug
            if (Debugger.isLocal() && StringUtils.equals(field.getName(), ConstantsFrame.ASYNC)) {
                value = false;
            }
            // local env, change timeout
            if (Debugger.isLocal() && StringUtils.equals(field.getName(), ConstantsFrame.PROCESSOR_TIMEOUT)) {
                value = ConstantsFrame.PROCESSOR_DEBUG_TIMEOUT_MS;
            }
            if (Objects.nonNull(value) && field.getGenericType() != value.getClass()) {
                value = TypeUtils.cast(value, field.getGenericType(), ParserConfig.getGlobalInstance());
            }
        } catch (Exception e) {
            Debugger.exception(instance, StatusType.CONFIG_INIT_EXCEPTION, e);
        }
        return value;
    }
}
