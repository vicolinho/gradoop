package org.gradoop.model.impl.algorithms.fsm.decoders;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;

import java.util.List;

public interface TransactionalFsmDecoder<T> {

  T decode(
    DataSet<WithCount<CompressedSubgraph>> frequentSubgraphs,
    DataSet<List<String>> vertexLabelDictionary,
    DataSet<List<String>> edgeLabelDictionary
  );
}
