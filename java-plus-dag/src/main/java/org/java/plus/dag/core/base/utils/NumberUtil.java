package org.java.plus.dag.core.base.utils;

import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;

/**
 * @author youku
 * @date 2019/11/07
 */
public class NumberUtil {
    /**
     * ����ΪnullתΪ�մ�,���򷵻���ʵֵ
     */
    public static String longToString(Long oriRes) {
        if (oriRes == null) {
            return StringUtils.EMPTY;
        }
        return oriRes.toString();
    }

    public static boolean isGreaterThanOrEqual(Number num1, Number num2) {
        if (num1 == null && num2 == null) {
            return true;
        } else if (num1 == null) {
            return false;
        } else if (num2 == null) {
            return true;
        } else {
            BigDecimal b1 = new BigDecimal(num1.doubleValue());
            BigDecimal b2 = new BigDecimal(num2.doubleValue());
            return b1.compareTo(b2) >= 0;
        }
    }
}
