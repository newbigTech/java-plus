//package org.java.plus.dag.core.base.utils.tair;
//
//import com.taobao.tair.impl.mc.MultiClusterTairManager;
//import org.apache.commons.lang3.StringUtils;
//
//public class TairParserFactory {
//
//    public static TairParser getParser(MultiClusterTairManager tairManager, int nameSpace, String pkey, String skey) {
//        if (StringUtils.isBlank(pkey)) {
//            throw new IllegalArgumentException("tair pkey should not be empty");
//        }
//        if (StringUtils.isBlank(skey)) {
//            return TairPkeyParser.from(tairManager, nameSpace, pkey);
//        }
//        return TairPkeySkeyParser.from(tairManager, nameSpace, pkey, skey);
//    }
//
//    public static TairParser getParser(MultiClusterTairManager tairManager, int nameSpace, String pkey) {
//        return getParser(tairManager, nameSpace, pkey, StringUtils.EMPTY);
//    }
//}
