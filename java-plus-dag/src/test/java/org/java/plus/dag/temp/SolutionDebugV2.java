package org.java.plus.dag.temp;
 
import java.util.Map;
 
import com.google.common.collect.Maps;

import mockit.Mock;
import mockit.MockUp; 

/**
 * @author seven.wxy
 * @date 2018/9/19
 */
public class SolutionDebugV2   {
//    public void mockTppConfig() {
//        new MockUp<SdkEnvironment>() {
//            @Mock
//            public long getSceneId() {return 16714;}
//
//            @Mock
//            public long getAbId() {return 140254;}
//
//            @Mock
//            public long getSolutionId() {return 23914;}
//        };
//    }
//
//    void mockSolutionClassV2() {
//        new MockUp<SdkEnvironment>() { 
//            public String getSolutionClass() {
//                return "com.taobao.recommendplatform.solutions.ykrcmd.rec_chain_solution.CommonChainSolution";
//            }
//        };
//    }
 
    public void testRecommend() throws Exception {
//        mockTppConfig();
//        mockTppConfig("140254");
//        mockSolutionClassV2();
        Map<String, String> params = Maps.newHashMap();
        params.put("utdid", "AACd0S3sbS8DAB2NLGyCtlDw");
        params.put("uid", "907075934");
        params.put("content_id", "10108");
        params.put("appid", "16714");
        params.put("app_id", "16714");
        params.put("count", "10");
        params.put("adsPageNo", "10");
        params.put("pn", "1");
        params.put("vid", "1069575400");
        params.put("scg_id", "7890601");
        params.put("cate", "87");
        params.put("appname", "youku");
        params.put("debugParam", "fbk:size_exception");
        params.put("env", "local"); //required in local debug, make async service to sync service
  
//        recommend(params);
    }
}
