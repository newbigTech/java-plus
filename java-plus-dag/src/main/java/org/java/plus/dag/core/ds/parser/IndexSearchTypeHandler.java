package org.java.plus.dag.core.ds.parser;

//import com.taobao.igraph.client.model.AtomicQuery;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
import org.java.plus.dag.core.ds.model.IndexSearchPojo;
import org.java.plus.dag.core.base.utils.Logger;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IndexSearchTypeHandler
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/11/30 11:32 AM
 */
public class IndexSearchTypeHandler extends BaseTypeHandler {

    private IndexSearchPojo indexSearchPojo;

    public IndexSearchTypeHandler(IndexSearchPojo indexSearchPojo) {
        this.indexSearchPojo = indexSearchPojo;
    }

	@Override
	IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
		// TODO Auto-generated method stub
		return null;
	}

//    private static AtomicQuery buildAtomQuery(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                              String indexSearch, int range) {
//        AtomicQuery atomicQuery = new AtomicQuery("TPP_" + iGraphDataSourceConfigPojo.getTableName(), indexSearch);
//        atomicQuery.setRange(0, range);
//        atomicQuery.setReturnFields(iGraphDataSourceConfigPojo.getIGraphQueryField());
//
//        Logger.info(() -> "atomicQuery:" + atomicQuery);
//
//        return atomicQuery;
//    }

//    @Override
//    IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
//        return IGraphRequest.from(
//                buildAtomQuery(iGraphDataSourceConfig.getSingleConfig(), indexSearchPojo.getIndexSearchString(),
//                        indexSearchPojo.getSize()));
//    }
}
