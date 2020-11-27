package com.dag.scheduler;

import java.util.HashMap;
import java.util.Map;
import com.tuhu.algo.etl.features.core.FeatureConfig;

import com.tuhu.algo.etl.features.pojo.Feature;

import com.tuhu.algo.etl.features.enums.RawFeatureType;


public class Test1 {

	public static void main(String[] args) { 
		FeatureConfig featureConfig=new FeatureConfig(0, null, null);
		
		 Map<String, Object> valueMap =new HashMap<>();
        double[] prdFeatureDouble = featureConfig.transformToArray(valueMap);
	}
}
