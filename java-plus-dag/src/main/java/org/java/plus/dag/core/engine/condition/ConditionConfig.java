package org.java.plus.dag.core.engine.condition;

import lombok.Getter;

import java.util.Objects;

/**
 * @author seven.wxy
 * @date 2019/1/30
 */
public class ConditionConfig {
    @Getter
    private String conditionKey;
    @Getter
    private String expectedValue;

    public ConditionConfig(String conditionKey, String expectedValue) {
        this.conditionKey = conditionKey;
        this.expectedValue = expectedValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConditionConfig that = (ConditionConfig) o;

        if (!Objects.equals(conditionKey, that.conditionKey)) {
            return false;
        }
        return Objects.equals(expectedValue, that.expectedValue);
    }

    @Override
    public int hashCode() {
        int result = conditionKey != null ? conditionKey.hashCode() : 0;
        result = 31 * result + (expectedValue != null ? expectedValue.hashCode() : 0);
        return result;
    }
}
