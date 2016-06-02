package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;

import java.util.List;

public interface TransactionalFSMDecoder<T> {

  T decode(
    DataSet<Supportable<CompressedDfsCode>> frequentSubgraphs,
    DataSet<List<String>> vertexLabelDictionary,
    DataSet<List<String>> edgeLabelDictionary
  );
}
