package org.gradoop.model.impl.algorithms.fsm.encoders;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;

import java.util.List;

public interface TransactionalFsmEncoder<T> {



  DataSet<EdgeTriple> encode(T input, FsmConfig fsmConfig);

  DataSet<Integer> getMinFrequency();

  DataSet<List<String>> getVertexLabelDictionary();

  DataSet<List<String>> getEdgeLabelDictionary();

}
