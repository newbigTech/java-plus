package org.java.plus.dag.core.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.java.plus.dag.core.base.utils.Logger;

/**
 * DagStruct
 * org.java.plus.dag.frame.engine.DagStruct
 *
 * @author jaye
 * @date 2018/10/11
 * <p>
 * config_start:
 * |org.java.plus.dag.frame.engine.DagStruct|DagStruct|jaye|
 * config_end:
 */
public class DagStruct<T> implements Cloneable {
    private static final int DEFAULT_VERTEX_COUNT = 60;
    private static final int ACCESSIBLE = 1;
    private static final int UN_ACCESSIBLE = 0;
    private static final float SPACE_EXPAND_FACTOR = 1.5f;

    class Vertex {
        String vertexKey;
        T vertex;

        Vertex(String vertexKey, T vertex) {
            this.vertexKey = vertexKey;
            this.vertex = vertex;
        }
    }

    private List<Vertex> mVertexList;
    private Map<String, Integer> mVertexIndex;
    private int[][] mEdges;
    private List<String> mStartPoints;
    private String mEndPoint;
    private boolean mIsInit;

    public DagStruct() {
        mVertexList = new ArrayList<>(DEFAULT_VERTEX_COUNT);
        mVertexIndex = new HashMap<>(DEFAULT_VERTEX_COUNT);
        mEdges = new int[DEFAULT_VERTEX_COUNT][DEFAULT_VERTEX_COUNT];
        mStartPoints = new ArrayList<>();
    }

    private DagStruct(DagStruct<T> dag, Function<T, T> vertexCloneFunc) {
        this.mVertexList = new ArrayList<>(dag.mVertexList.size());
        dag.mVertexList.forEach(v -> this.mVertexList.add(new Vertex(v.vertexKey, vertexCloneFunc.apply(v.vertex))));
        this.mVertexIndex = dag.mVertexIndex;
        this.mEdges = dag.mEdges;
        this.mStartPoints = dag.mStartPoints;
        this.mEndPoint = dag.mEndPoint;
        this.mIsInit = dag.mIsInit;
    }

    public DagStruct<T> cloneDag(Function<T, T> vertexCloneFunc) {
        return new DagStruct<>(this, vertexCloneFunc);
    }

    public void forEachVertex(Consumer<T> consumer) {
        mVertexList.forEach(v -> consumer.accept(v.vertex));
    }

    private boolean isStartPoint(int idx) {
        for (int fromIdx = 0; fromIdx < mVertexList.size(); fromIdx++) {
            if (ACCESSIBLE == mEdges[fromIdx][idx]) {
                return false;
            }
        }
        return true;
    }

    private boolean isEndPoint(int idx) {
        for (int toIdx = 0; toIdx < mVertexList.size(); toIdx++) {
            if (ACCESSIBLE == mEdges[idx][toIdx]) {
                return false;
            }
        }
        return true;
    }

    private String getVertexName(int vertexIndex) {
        for (Map.Entry<String, Integer> entry : mVertexIndex.entrySet()) {
            if (0 == entry.getValue().compareTo(vertexIndex)) {
                return entry.getKey();
            }
        }
        return "";
    }

    public boolean initStartAndEndPoint() {
        if (mVertexList.size() <= 0) {
            Logger.warn(() -> "DAG: no vertex or edge is not init");
            return false;
        }
        for (int i = 0; i < mVertexList.size(); i++) {
            if (isStartPoint(i)) {
                mStartPoints.add(mVertexList.get(i).vertexKey);
            }
            if (isEndPoint(i)) {
                if (null != mEndPoint) {
                    final int iFinal = i;
                    Logger.warn(() -> String.format("DAG: multi end, [%s] and [%s]", mEndPoint, getVertexName(iFinal)));
                    return false;
                }
                mEndPoint = mVertexList.get(i).vertexKey;
            }
        }
        return true;
    }

    private boolean topologicalSort() {
        int[] inDegree = new int[mVertexList.size()];
        List<Integer> queue = new ArrayList<>();

        for (int fromIdx = 0; fromIdx < mVertexList.size(); fromIdx++) {
            for (int toIdx = 0; toIdx < mVertexList.size(); toIdx++) {
                if (ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                    inDegree[toIdx]++;
                }
            }
        }
        for (int i = 0; i < mVertexList.size(); i++) {
            if (0 == inDegree[i]) {
                queue.add(i);
            }
        }

        List<Integer> path = new ArrayList<>(mVertexList.size());
        while (queue.size() > 0) {
            int visitedIdx = queue.remove(0);
            path.add(visitedIdx);
            for (int toIdx = 0; toIdx < mVertexList.size(); toIdx++) {
                if (ACCESSIBLE == mEdges[visitedIdx][toIdx]) {
                    if (0 == --inDegree[toIdx]) {
                        queue.add(toIdx);
                    }
                }
            }
        }

        return path.size() == mVertexList.size();
    }

    public DagStruct addVertex(String vertexKey, T vertex) {
        if (null != mVertexIndex.get(vertexKey)) {
            int idx = mVertexIndex.get(vertexKey);
            mVertexList.get(idx).vertex = vertex;
            return this;
        }
        mVertexList.add(new Vertex(vertexKey, vertex));
        mVertexIndex.put(vertexKey, mVertexList.size() - 1);
        if (mVertexList.size() > mEdges.length) {
            int space = (int)(mVertexList.size() * SPACE_EXPAND_FACTOR);
            int[][] newEdges = new int[space][space];
            for (int i = 0; i < mEdges.length; ++i) {
                System.arraycopy(mEdges[i], 0, newEdges[i], 0, mEdges.length);
            }
            mEdges = newEdges;
        }
        return this;
    }

    /**
     * use addVertex instead
     */
    @Deprecated
    public DagStruct replaceOrAddVertex(String vertexKey, T vertex) {
        return addVertex(vertexKey, vertex);
    }

    public DagStruct addEdge(String vexFrom, String vexTo) {
        if (null == mVertexIndex.get(vexFrom) || null == mVertexIndex.get(vexTo)) {
            return this;
        }
        mEdges[mVertexIndex.get(vexFrom)][mVertexIndex.get(vexTo)] = ACCESSIBLE;
        return this;
    }

    public List<T> getStartPoint() {
        List<T> vertexList = new ArrayList<>(mStartPoints.size());
        for (String vertexKey : mStartPoints) {
            T vertex = mVertexList.get(mVertexIndex.get(vertexKey)).vertex;
            vertexList.add(vertex);
        }
        return vertexList;
    }

    public List<String> getStartPointKey() {
        return mStartPoints;
    }

    public T getEndPoint() {
        if (null == mEndPoint) {
            return null;
        }
        return mVertexList.get(mVertexIndex.get(mEndPoint)).vertex;
    }

    public String getEndPointKey() {
        return mEndPoint;
    }

    @Deprecated
    public T getProcessor(String vertexKey) { return getVertex(vertexKey); }

    public T getVertex(String vertexKey) {
        if (!mVertexIndex.containsKey(vertexKey)) {
            return null;
        }
        return mVertexList.get(mVertexIndex.get(vertexKey)).vertex;
    }

    @Deprecated
    public List<T> getFollowingProcessor(String vertexKey) { return getFollowingVertex(vertexKey); }

    public List<T> getFollowingVertex(String vertexKey) {
        List<T> following = new ArrayList<>();
        Integer fromIdx = mVertexIndex.get(vertexKey);
        if (null == fromIdx) {
            return following;
        }
        for (int toIdx = 0; toIdx < mVertexList.size(); toIdx++) {
            if (UN_ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                continue;
            }
            following.add(mVertexList.get(toIdx).vertex);
        }
        return following;
    }

    public void forEachFollowingVertex(String vertexKey, Consumer<T> consumer) {
        Integer fromIdx = mVertexIndex.get(vertexKey);
        if (null == fromIdx) { return; }
        for (int toIdx = 0; toIdx < mVertexList.size(); toIdx++) {
            if (UN_ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                continue;
            }
            consumer.accept(mVertexList.get(toIdx).vertex);
        }
    }

    public List<String> getFollowingKey(String vertexKey) {
        List<String> following = new ArrayList<>();
        Integer fromIdx = mVertexIndex.get(vertexKey);
        if (null == fromIdx) {
            return following;
        }
        for (int toIdx = 0; toIdx < mVertexList.size(); toIdx++) {
            if (UN_ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                continue;
            }
            following.add(mVertexList.get(toIdx).vertexKey);
        }
        return following;
    }

    @Deprecated
    public List<T> getDependedProcessor(String vertexKey) { return getDependedVertex(vertexKey); }

    public List<T> getDependedVertex(String vertexKey) {
        List<T> depended = new ArrayList<>();
        Integer toIdx = mVertexIndex.get(vertexKey);
        if (null == toIdx) {
            return depended;
        }
        for (int fromIdx = 0; fromIdx < mVertexList.size(); fromIdx++) {
            if (ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                depended.add(mVertexList.get(fromIdx).vertex);
            }
        }
        return depended;
    }

    public void forEachDependedVertex(String vertexKey, Consumer<T> consumer) {
        Integer toIdx = mVertexIndex.get(vertexKey);
        if (null == toIdx) { return; }
        for (int fromIdx = 0; fromIdx < mVertexList.size(); fromIdx++) {
            if (ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                consumer.accept(mVertexList.get(fromIdx).vertex);
            }
        }
    }

    public List<String> getDependedKey(String vertexKey) {
        List<String> depended = new ArrayList<>();
        Integer toIdx = mVertexIndex.get(vertexKey);
        if (null == toIdx) {
            return depended;
        }
        for (int fromIdx = 0; fromIdx < mVertexList.size(); fromIdx++) {
            if (ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                depended.add(mVertexList.get(fromIdx).vertexKey);
            }
        }
        return depended;
    }

    public int getDependCount(String vertexKey) {
        int dependCount = 0;
        Integer toIdx = mVertexIndex.get(vertexKey);
        if (null == toIdx) {
            return dependCount;
        }
        for (int fromIdx = 0; fromIdx < mVertexList.size(); ++fromIdx) {
            if (ACCESSIBLE == mEdges[fromIdx][toIdx]) {
                ++dependCount;
            }
        }
        return dependCount;
    }

    public boolean isInit() {
        return mIsInit;
    }

    public boolean init() {
        if (mIsInit) {
            return true;
        }
        if (!initStartAndEndPoint()) {
            return false;
        }
        mIsInit = topologicalSort();
        if (!mIsInit) {
            Logger.warn(() -> "DAG: topological sort failed, may be there is cyclic dependency");
        }
        return mIsInit;
    }
}