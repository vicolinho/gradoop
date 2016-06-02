package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.SerializedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;

public class PostPruneAndCompress
  implements FlatMapFunction<ObjectWithCount<SerializedSubgraph>, ObjectWithCount<CompressedSubgraph>> {

  private final FSMConfig fsmConfig;

  public PostPruneAndCompress(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(ObjectWithCount<SerializedSubgraph> subgraphWithSupport,
    Collector<ObjectWithCount<CompressedSubgraph>> collector) throws Exception {

    DfsCode code = subgraphWithSupport.getObject().getDfsCode();
    int support = subgraphWithSupport.getSupport();

    if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
      collector.collect(new ObjectWithCount<>(new CompressedSubgraph(code), support));
    }
  }
}
