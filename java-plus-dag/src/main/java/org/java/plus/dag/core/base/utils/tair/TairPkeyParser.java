//package org.java.plus.dag.core.base.utils.tair;
//
//import com.taobao.tair.DataEntry;
//import com.taobao.tair.Result;
//import com.taobao.tair.impl.mc.MultiClusterTairManager;
//import org.apache.commons.collections4.CollectionUtils;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class TairPkeyParser implements TairParser {
//
//    private MultiClusterTairManager tairManager;
//    private int nameSpace;
//    private List<String> pkeys;
//
//    private TairPkeyParser(MultiClusterTairManager tairManager, int nameSpace, String... pkey) {
//        this.tairManager = tairManager;
//        this.nameSpace = nameSpace;
//        this.pkeys = pkey == null ? Collections.emptyList() : Arrays.asList(pkey);
//    }
//
//    public static TairParser from(MultiClusterTairManager tairManager, int nameSpace, String... pkey) {
//        return new TairPkeyParser(tairManager, nameSpace, pkey);
//    }
//
//    @Override
//    public Result<DataEntry> retrieve() {
//        int size = pkeys.size();
//        if (size == 0) {
//            return null;
//        } else if (size == 1) {
//            return tairManager.get(nameSpace, pkeys.get(0));
//        } else {
//            Result<List<DataEntry>> data = tairManager.mget(nameSpace, pkeys);
//            return data == null ? null : new Result<>(data.getRc(), CollectionUtils.isEmpty(data.getValue()) ? null : data
//                    .getValue()
//                    .get(0), data.getFlag());
//        }
//    }
//
//    @Override
//    public Result<List<DataEntry>> retrieveAll() {
//        return tairManager.mget(nameSpace, pkeys);
//    }
//
//    @Override
//    public Result<Map<Object, Map<Object, Result<DataEntry>>>> retrieveMap() {
//        Result<List<DataEntry>> data = tairManager.mget(nameSpace, pkeys);
//        if (data == null) {
//            return null;
//        } else if (CollectionUtils.isEmpty(data.getValue())) {
//            return new Result<>(data.getRc(), null, data.getFlag());
//        } else {
//            int size = data.getValue().size();
//            Map<Object, Map<Object, Result<DataEntry>>> map = new HashMap<>(size);
//            for (int i = 0; i < size; i++) {
//                Map<Object, Result<DataEntry>> tmp = new HashMap<>(1);
//                tmp.put(StringUtils.EMPTY, new Result<>(data.getRc(), data.getValue().get(i), data.getFlag()));
//                map.put(pkeys.get(i), tmp);
//            }
//            return new Result<>(data.getRc(), map, data.getFlag());
//        }
//    }
//}
