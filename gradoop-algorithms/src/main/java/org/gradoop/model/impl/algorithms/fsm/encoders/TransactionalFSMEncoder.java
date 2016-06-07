package org.gradoop.model.impl.algorithms.fsm.encoders;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;

import java.util.List;

public interface TransactionalFSMEncoder<T> {



  DataSet<EdgeTriple> encode(T input, FSMConfig fsmConfig);

  DataSet<Integer> getMinFrequency();

  DataSet<List<String>> getVertexLabelDictionary();

  DataSet<List<String>> getEdgeLabelDictionary();

}
