//package org.java.plus.dag.core.base.utils.tair;
//
//import java.util.Objects;
//
//import org.java.plus.dag.core.base.utils.Debugger;
//import org.java.plus.dag.core.base.utils.TppObjectFactory;
//import org.java.plus.dag.core.ds.TairDataSourceBase;
//import org.java.plus.dag.core.ds.model.TairDataSourceConfig;
//import org.apache.commons.lang.StringUtils;
//import org.jetbrains.annotations.NotNull;
//
//import static org.java.plus.dag.core.base.utils.tair.TairClientImpl.TEST_UNIT;
//
//public class TairUtil {
//    public static final String USER_NAME_4023 = "real_tair_user_name";
//    public static final String UNIT_4023 = "zbyk";
//
//    @NotNull
//    public static TairClient getUnifiedTair() {
//        return TppObjectFactory.getBeanCache()
//            .values()
//            .stream()
//            .filter(e -> e instanceof TairDataSourceBase)
//            .filter(e -> !Objects.isNull(((TairDataSourceBase)e).getTairDataSourceConfig())
//                && StringUtils.equals(USER_NAME_4023, ((TairDataSourceBase)e).getTairDataSourceConfig().getUserName()))
//            .findFirst()
//            .map(e -> ((TairDataSourceBase)e).getTairClient())
//            .orElseGet(() -> TairClientImpl.getInstance(new TairDataSourceConfig(USER_NAME_4023, 50, 4023,
//                Debugger.isLocal() ? TEST_UNIT : UNIT_4023)));
//    }
//}
