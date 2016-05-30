package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.util.Collection;

public class ExpandFrequentSubgraphs implements
  FlatMapFunction<Collection<CompressedDFSCode>, CompressedDFSCode> {

  @Override
  public void flatMap(Collection<CompressedDFSCode> compressedDFSCodes,
    Collector<CompressedDFSCode> collector) throws Exception {

    for(CompressedDFSCode compressedDFSCode : compressedDFSCodes) {

      compressedDFSCode.setSupport(
        compressedDFSCode.getDfsCode().getMinVertexLabel());

      collector.collect(compressedDFSCode);
    }
  }
}
