package org.java.plus.dag.core.ds.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import org.java.plus.dag.core.ds.model.KeyPair;
import org.apache.commons.collections4.CollectionUtils;

public class IGraphReplaceHandler implements ReplaceHandler {
	public static final Splitter COMMA_SPLITTER = Splitter.on(",");

	@Override
	public List<KeyPair> replace(List<Map<String, Object>> dataSetReplaceResult) {
		if (CollectionUtils.isEmpty(dataSetReplaceResult)) {
			return new ArrayList<>();
		}
		// 通过List<Map<String, Object>> 构建key Values 对象
		// list 为多行结果，每行结果 为 $.pkey 和 替换后的值
		// 取出我们所需要的pKey 和 sKey 即可
		// TODO: 2019-01-15 pKey sKey是可变化的 需要该类针对修改 可拓展
		return dataSetReplaceResult.stream().map(dataMap -> {
			KeyPair keyPair = null;
			for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
				if (entry.getKey().contains("pkey")) {
					keyPair = KeyPair.from((String) entry.getValue());
				} else if (entry.getKey().contains("skey")) {
					List<String> sKey = COMMA_SPLITTER.splitToList((String) entry.getValue());
					keyPair = KeyPair.from(sKey);
				}
			}
			return keyPair;
		}).filter(Objects::nonNull).collect(Collectors.toList());
	}

}
