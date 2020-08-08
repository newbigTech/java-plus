package org.java.plus.dag.core.engine;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.java.plus.dag.core.engine.condition.ConditionContext;

/**
 * Dag engine interface
 *
 * @author seven.wxy
 * @date 2019/3/28
 */
public interface DagEngine<CONTEXT> {
    /**
     * Dag concurrent thread count per request
     *
     * @param threadCntPerQuery concurrent thread count
     */
    void setThreadCntPerQuery(int threadCntPerQuery);

    /**
     * Dag timeout ms
     *
     * @param timeout timeout milliseconds
     */
    void setTimeout(int timeout);

    /**
     * Dag timeout remove wait time
     *
     * @param removeWaitTime remove wait time or not
     */
    void setRemoveWaitTime(boolean removeWaitTime);

    /**
     * Use multi thread execute or not
     *
     * @param async async or not
     */
    void setAsync(boolean async);

    /**
     * Skip timeout processor or not
     *
     * @param skipTimeout skip timeout processor or not
     */
    void setSkipTimeout(boolean skipTimeout);

    /**
     * Init dag struct, call this method before {@link #run(Object, Object)}
     *
     * @return is init success or not
     */
    boolean initDagStruct();

    /**
     * Run dag graph
     *
     * @param <OUTPUT> output type
     * @return output data
     * @throws Exception
     */
    <OUTPUT> OUTPUT run() throws Exception;

    /**
     * Run dag graph with input data and context
     *
     * @param context  execute context
     * @param input    init input
     * @param <INPUT>  input type
     * @param <OUTPUT> output type
     * @return output data
     * @throws Exception
     */
    <INPUT, OUTPUT> OUTPUT run(CONTEXT context, INPUT input) throws Exception;

    /**
     * Merge parallel result function, OUTPUT_A and OUTPUT_B not ensure order which {@link #addDependence}
     * Suggest the same type result to merge, if not, use Map type return
     *
     * @param mergeFunction merge function
     * @param <OUTPUT_A>    type of output one
     * @param <OUTPUT_B>    type of output another
     * @param <OUTPUT>      type of merge result
     */
    <OUTPUT_A, OUTPUT_B, OUTPUT> void setMergeFunction(BiFunction<OUTPUT_A, OUTPUT_B, OUTPUT> mergeFunction);

    /**
     * Condition predicate map, key=functionUniqueId, value=Predicate
     *
     * @param conditions
     */
    void setConditions(Map<String, Predicate<ConditionContext>> conditions);

    /**
     * Add node with unique id dependence to dag
     *
     * @param fromFunctionUniqueId from node unique id
     * @param from                 from node function
     * @param toFunctionUniqueId   to node unique id
     * @param to                   to node function
     * @param <INPUT_F>            from node input type
     * @param <INPUT_T>            to node input type
     * @param <OUTPUT_F>           from node output type
     * @param <OUTPUT_T>           to node output type
     * @return current object
     */
    <INPUT_F, INPUT_T, OUTPUT_F, OUTPUT_T> DagEngine addDependence(String fromFunctionUniqueId,
                                                                   DagFunction<CONTEXT, INPUT_F, OUTPUT_F> from,
                                                                   String toFunctionUniqueId,
                                                                   DagFunction<CONTEXT, INPUT_T, OUTPUT_T> to);

    /**
     * Add function node with unique id to dag, no dependence
     *
     * @param functionUniqueId function node unique id
     * @param function         to add function
     * @param <INPUT>          input type
     * @param <OUTPUT>         output type
     * @return current object
     */
    <INPUT, OUTPUT> DagEngine addNode(String functionUniqueId, DagFunction<CONTEXT, INPUT, OUTPUT> function);

    /**
     * Return the dag struct
     * @param <T>
     * @return
     */
    <T> DagStruct<T> getDag();
}
