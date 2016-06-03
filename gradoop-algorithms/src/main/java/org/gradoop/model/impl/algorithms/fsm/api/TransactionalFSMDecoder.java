package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.WithCount;

import java.util.List;

public interface TransactionalFSMDecoder<T> {

  T decode(
    DataSet<WithCount<CompressedSubgraph>> frequentSubgraphs,
    DataSet<List<String>> vertexLabelDictionary,
    DataSet<List<String>> edgeLabelDictionary
  );
}
