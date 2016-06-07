package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.SerializedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;

public class PostPruneAndCompress
  implements FlatMapFunction<WithCount<SerializedSubgraph>, WithCount<CompressedSubgraph>> {

  private final FSMConfig fsmConfig;

  public PostPruneAndCompress(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(WithCount<SerializedSubgraph> subgraphWithSupport,
    Collector<WithCount<CompressedSubgraph>> collector) throws Exception {

    DfsCode code = subgraphWithSupport.getObject().getDfsCode();
    int support = subgraphWithSupport.getCount();

    if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
      collector.collect(new WithCount<>(new CompressedSubgraph(code), support));
    }
  }
}
