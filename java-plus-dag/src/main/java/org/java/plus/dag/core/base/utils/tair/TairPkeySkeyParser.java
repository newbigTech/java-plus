//package org.java.plus.dag.core.base.utils.tair;
//
//import com.google.common.collect.Maps;
//import com.taobao.tair.DataEntry;
//import com.taobao.tair.Result;
//import com.taobao.tair.impl.mc.MultiClusterTairManager;
//import com.tmall.crowd.guava.collect.Lists;
//import lombok.NonNull;
//import org.apache.commons.collections4.ListUtils;
//import org.apache.commons.collections4.MapUtils;
//import org.apache.commons.lang3.ObjectUtils;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.stream.Collectors;
//
//public class TairPkeySkeyParser implements TairParser {
//
//    private MultiClusterTairManager tairManager;
//    private int nameSpace;
//    private Map<String, List<String>> pkeySkeyListMap;
//
//    private TairPkeySkeyParser(MultiClusterTairManager tairManager, int nameSpace, String pkey, String skey) {
//        this.tairManager = tairManager;
//        this.nameSpace = nameSpace;
//        this.pkeySkeyListMap = new HashMap<>(1);
//        this.pkeySkeyListMap.put(pkey, Collections.singletonList(ObjectUtils.defaultIfNull(skey, StringUtils.EMPTY)));
//    }
//
//    private TairPkeySkeyParser(MultiClusterTairManager tairManager, int nameSpace, LinkedHashMap<String, List<String>> pkeySkeyListMap) {
//        this.tairManager = tairManager;
//        this.nameSpace = nameSpace;
//        this.pkeySkeyListMap = pkeySkeyListMap;
//    }
//
//    public static TairParser from(MultiClusterTairManager tairManager, int nameSpace, @NonNull String pkey, String skey) {
//        return new TairPkeySkeyParser(tairManager, nameSpace, pkey, skey);
//    }
//
//    public static TairParser from(MultiClusterTairManager tairManager, int nameSpace, LinkedHashMap<String, List<String>> pkeySkeys) {
//        pkeySkeys = pkeySkeys == null ? Maps.newLinkedHashMap() : pkeySkeys;
//        pkeySkeys.remove(null);
//        return new TairPkeySkeyParser(tairManager, nameSpace, pkeySkeys);
//    }
//
//    @Override
//    public Result<DataEntry> retrieve() {
//        if (pkeySkeyListMap.size() == 0) {
//            return null;
//        } else {
//            String pkey = Lists.newArrayList(pkeySkeyListMap.keySet()).get(0);
//            List<String> skeys = ListUtils.emptyIfNull(pkeySkeyListMap.get(pkey));
//            int size = skeys.size();
//            if (pkeySkeyListMap.size() == 1) {
//                if (size <= 1) {
//                    return tairManager.prefixGet(nameSpace, pkey, StringUtils.defaultIfEmpty(skeys.get(0), StringUtils.EMPTY));
//                } else {
//                    Result<Map<Object, Result<DataEntry>>> result = tairManager.prefixGets(nameSpace, pkey, skeys);
//                    if (result != null && MapUtils.emptyIfNull(result.getValue()).containsKey(skeys.get(0))) {
//                        return result.getValue().get(skeys.get(0));
//                    }
//                    return result == null ? null : new Result<>(result.getRc(), null, result.getFlag());
//                }
//            } else {
//                Result<Map<Object, Map<Object, Result<DataEntry>>>> result = tairManager.mprefixGets(nameSpace, pkeySkeyListMap);
//                if (result != null && MapUtils.emptyIfNull(MapUtils.emptyIfNull(result.getValue()).get(pkey))
//                                              .containsKey(skeys.get(0))) {
//                    return result.getValue().get(pkey).get(skeys.get(0));
//                } else {
//                    return result == null ? null : new Result<>(result.getRc(), null, result.getFlag());
//                }
//            }
//        }
//    }
//
//    @Override
//    public Result<List<DataEntry>> retrieveAll() {
//        if (pkeySkeyListMap.size() == 0) {
//            return null;
//        } else {
//            String pkey = Lists.newArrayList(pkeySkeyListMap.keySet()).get(0);
//            List<String> skeys = ListUtils.emptyIfNull(pkeySkeyListMap.get(pkey));
//            int size = skeys.size();
//            if (pkeySkeyListMap.size() == 1) {
//                if (size <= 1) {
//                    Result<DataEntry> result = tairManager.prefixGet(nameSpace, pkey, ObjectUtils.defaultIfNull(skeys.get(0), StringUtils.EMPTY));
//                    return result == null ? null : new Result<>(result.getRc(), Collections.singletonList(result.getValue()), result
//                            .getFlag());
//                } else {
//                    Result<Map<Object, Result<DataEntry>>> result = tairManager.prefixGets(nameSpace, pkey, skeys);
//                    return result == null || MapUtils.emptyIfNull(result.getValue()) == null ? null : new Result<>(result
//                            .getRc(),
//                            MapUtils.emptyIfNull(result.getValue())
//                                    .values()
//                                    .stream()
//                                    .filter(Objects::nonNull)
//                                    .map(Result::getValue)
//                                    .filter(Objects::nonNull)
//                                    .collect(Collectors.toList())
//                            , result.getFlag());
//                }
//            } else {
//                Result<Map<Object, Map<Object, Result<DataEntry>>>> result = tairManager.mprefixGets(nameSpace, pkeySkeyListMap);
//                if (result != null && MapUtils.isNotEmpty(MapUtils.emptyIfNull(result.getValue()).get(pkey))) {
//                    return new Result<>(result.getRc(), result.getValue()
//                                                              .get(pkey)
//                                                              .values()
//                                                              .stream()
//                                                              .filter(Objects::nonNull)
//                                                              .map(Result::getValue)
//                                                              .filter(Objects::nonNull)
//                                                              .collect(Collectors.toList()), result.getFlag());
//                }
//                return result == null ? null : new Result<>(result.getRc(), null, result.getFlag());
//            }
//        }
//    }
//
//    @Override
//    public Result<Map<Object, Map<Object, Result<DataEntry>>>> retrieveMap() {
//        return tairManager.mprefixGets(nameSpace, pkeySkeyListMap);
//    }
//}
