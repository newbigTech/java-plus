package org.java.plus.dag.core.dataflow.ops;

import java.util.ArrayList;
import java.util.List;

//import com.taobao.igraph.client.model.KeyList;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.java.plus.dag.core.ds.IGraphDataSourceBase;
import org.java.plus.dag.taobao.KeyList;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/14
 */
public class ReadIgraph extends Operation<DataSet<Row>> {
    private Operation<List<KeyList>> keyListOp;
    private List<KeyList> keyList = new ArrayList<>();
    private String igraphKey;

    public ReadIgraph(Operation<List<KeyList>> keyListOp, String igraphKey) {
        this.keyListOp = keyListOp;
        this.igraphKey = igraphKey;
    }

    public ReadIgraph(List<KeyList> keyList, String igraphKey) {
        this.keyList = keyList;
        this.igraphKey = igraphKey;
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        handleErr(e -> new DataSet<>());
        requireNonNull(igraphKey);
        if (!isNullOp(keyListOp)) {
            keyList = keyListOp.get();
        }
        IGraphDataSourceBase dataSource = TppObjectFactory.getBean(igraphKey, IGraphDataSourceBase.class);
        return dataSource.read(ctx, keyList);
    }
}