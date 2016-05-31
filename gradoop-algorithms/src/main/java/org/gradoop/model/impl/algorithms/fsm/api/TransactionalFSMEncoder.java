package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;

import java.util.List;

public interface TransactionalFSMEncoder<T> {



  DataSet<EdgeTriple> encode(T input, FSMConfig fsmConfig);

  DataSet<Integer> getMinSupport();

  DataSet<List<String>> getVertexLabelDictionary();

  DataSet<List<String>> getEdgeLabelDictionary();

}
