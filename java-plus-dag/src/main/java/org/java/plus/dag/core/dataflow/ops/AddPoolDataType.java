package org.java.plus.dag.core.dataflow.ops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import com.taobao.igraph.client.model.KeyList;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.java.plus.dag.taobao.KeyList;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/14
 */
public class AddPoolDataType extends Operation<DataSet<Row>> {
    private static final Map<String, Integer> SUPPORT_TYPES = new HashMap<String, Integer>(){{
        put("VIDEO", 1);
        put("SHOW", 2);
        put("SCG", 3);
    }};

    private Operation<DataSet<Row>> input;
    private String poolDataDsKey;

    public AddPoolDataType(Operation<DataSet<Row>> input, String poolDataDsKey) {
        this.input = input;
        this.poolDataDsKey = poolDataDsKey;
    }

    public Map<String, String> publishIdMap(DataSet<Row> poolData) {
        Map<String, String> result = new HashMap<>(poolData.getData().size());
        poolData.getData().forEach(row ->
            result.put(row.getFieldValue(AllFieldName.publishid), row.getFieldValue(AllFieldName.item_type))
        );
        return result;
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        handleErr(e -> input.get());
        requireOpNonNull(DataSet::new, input);
        List<KeyList> keyLists = new ArrayList<>();
        input.get().getData().forEach(row -> keyLists.add(new KeyList(row.getFieldValue(AllFieldName.publishid))));
        DataSet<Row> poolData = new ReadIgraph(keyLists, poolDataDsKey).eval(getCurProcessorName());
        Map<String, String> publishIdMap = publishIdMap(poolData);
        input.get().getData().forEach(row -> {
            String publishId = row.getFieldValue(AllFieldName.publishid);
            if (publishIdMap.containsKey(publishId)) {
                String type = publishIdMap.get(publishId);
                if (SUPPORT_TYPES.containsKey(type)) {
                    row.setType(SUPPORT_TYPES.get(type));
                }
            }
        });
        return input.get();
    }
}