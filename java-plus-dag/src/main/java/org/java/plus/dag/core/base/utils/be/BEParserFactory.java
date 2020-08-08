//package org.java.plus.dag.core.base.utils.be;
//
//import com.taobao.recommendplatform.protocol.service.dii.DIIRequest;
//import org.java.plus.dag.core.ds.model.BEDataSourceConfig;
//
///**
// * @author seth.zjw
// * @version V1.0
// * @Title: BEParserFactory
// * @Package org.java.plus.dag.frame.base.utils.be
// * @date 2018/10/10 ����2:50
// */
//public class BEParserFactory {
//
//    private String outfmt;
//    private BEDataSourceConfig beDataSourceConfig;
//
//    public BEParserFactory(BEDataSourceConfig beDataSourceConfig) {
//        this.beDataSourceConfig = beDataSourceConfig;
//        this.outfmt = beDataSourceConfig.getOutfmt();
//    }
//
//    public BEParser getParser() {
//        if (DIIRequest.FORMAT.FB2.toString().equalsIgnoreCase(outfmt)) {
//            return new FB2BEParser(beDataSourceConfig);
//        } else {
//            return new JsonBEParser(beDataSourceConfig);
//        }
//    }
//
//}
