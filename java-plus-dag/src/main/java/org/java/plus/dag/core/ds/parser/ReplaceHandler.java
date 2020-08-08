package org.java.plus.dag.core.ds.parser;

import org.java.plus.dag.core.ds.model.KeyPair;

import java.util.List;
import java.util.Map;

public interface ReplaceHandler {

    List<KeyPair> replace(List<Map<String, Object>> dataSetReplaceResult);
}
