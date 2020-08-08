package org.java.plus.dag.core.base.utils.tair;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.java.plus.dag.core.base.model.ProcessorContext;
//import com.taobao.tair.ResultCode;

public interface TairClient {
    Map<String, Serializable> getData(ProcessorContext context, List<String> pKeys);

    Serializable getData(ProcessorContext context, String key, int expire);

    Serializable getData(ProcessorContext context, String key);

    boolean deleteDataBatch(List<String> keys);

    boolean deleteData(String key);

    boolean putData(String key, Serializable value, int expireTime);

    boolean putData(String key, Serializable value);

    boolean putData(String key, Serializable value, int version, int expireTime);

    boolean putDataCheckVersion(ProcessorContext context, String key, Serializable value, int expireTime,
                                ILdbDataMerge merge);

//    ResultCode putDataWithVersionCheck(ProcessorContext context, String key, Serializable value, int expireTime);

    Serializable getDataWithPkeySkey(ProcessorContext context, String pKey, String sKey);

    Map<String, Map<String, Serializable>> getDataWithMutilPkeySkey(ProcessorContext context,
                                                                    LinkedHashMap<String, List<String>> pKeysKeyMap);

    boolean putDatas(String pKey, Map<Object, Object> sKeys);

    Integer incr(String key, int value, int defaultValue, int expireTime);
}
