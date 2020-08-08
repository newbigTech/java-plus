package org.java.plus.dag.core.dataflow.ops;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/14
 */
public class IdMap extends Operation<Map<String, Row>> {
    private Operation<DataSet<Row>> input;
    private DataSet<Row> inputDs;
    private Function<Throwable, Map<String, Row>> errHandler = e -> {
        logWarn("exception ocurred, " + e.getMessage());
        return new HashMap<>();
    };

    public IdMap(Operation<DataSet<Row>> input) { this.input = input; }
    public IdMap(DataSet<Row> inputDs) { this.inputDs = inputDs; }

    private Map<String, Row> idMap() {
        Map<String, Row> result = new HashMap<>(inputDs.getData().size());
        inputDs.getData().forEach(row -> result.put(row.getId(), row));
        return result;
    }

    @Override
    public Map<String, Row> apply(ProcessorContext ctx) {
        handleErr(errHandler);
        if (null != inputDs) {
            return idMap();
        }
        requireOpNonNull(input);
        inputDs = input.get();
        return idMap();
    }
}