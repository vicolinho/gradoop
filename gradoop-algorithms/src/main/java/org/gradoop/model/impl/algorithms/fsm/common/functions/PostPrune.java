package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

public class PostPrune
  implements FlatMapFunction<CompressedDfsCode, CompressedDfsCode> {

  private final FSMConfig fsmConfig;

  public PostPrune(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(CompressedDfsCode subgraph,
    Collector<CompressedDfsCode> collector) throws Exception {

    if (GSpan.isMinimumDfsCode(subgraph, fsmConfig)) {
      collector.collect(subgraph);
    }
  }
}
