package org.java.plus.dag.core.base.proc;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.InitUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * @author seven.wxy
 * @date 2019/7/10
 */
public abstract class AbstractInit {
    /** instance unique key */
    @Getter
    @Setter
    protected String instanceKey = StringUtils.EMPTY;
    @Getter
    @Setter
    @ConfigInit(desc = "instance key prefix")
    protected String instanceKeyPrefix = StringUtils.EMPTY;
    @Getter
    @Setter
    protected ProcessorConfig processorConfig;

    protected List<Field> allFieldsList = Collections.emptyList();

    /**
     * do init function, execute only once at instance create
     *
     * @param processorConfig
     */
    public abstract void doInit(ProcessorConfig processorConfig);

    public void init(ProcessorConfig processorConfig) {
        allFieldsList = FieldUtils.getAllFieldsList(this.getClass());
        this.reflectInitField(processorConfig);
        this.doInit(processorConfig);
    }

    /**
     * Scan @ConfigInit annotation to auto write config value
     *
     * @param processorConfig
     */
    private void reflectInitField(ProcessorConfig processorConfig) {
        this.processorConfig = processorConfig;
        for (Field field : allFieldsList) {
            ConfigInit reflectInit = field.getAnnotation(ConfigInit.class);
            if (Objects.isNull(reflectInit)) {
                continue;
            }
            try {
                Object value = InitUtils.getFieldValue(this, field, reflectInit, processorConfig);
                FieldUtils.writeField(this, field.getName(), value, true);
            } catch (Exception e) {
                Debugger.exception(this, StatusType.CONFIG_INIT_EXCEPTION, e);
            }
        }
    }
}
