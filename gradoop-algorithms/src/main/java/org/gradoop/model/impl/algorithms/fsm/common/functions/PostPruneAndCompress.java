package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;

public class PostPruneAndCompress
  implements FlatMapFunction<Supportable<DfsCode>, Supportable<CompressedDfsCode>> {

  private final FSMConfig fsmConfig;

  public PostPruneAndCompress(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(Supportable<DfsCode> subgraph,
    Collector<Supportable<CompressedDfsCode>> collector) throws Exception {

    DfsCode code = subgraph.getObject();
    if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
      collector.collect(new Supportable<>(new CompressedDfsCode(code)));
    }
  }
}
