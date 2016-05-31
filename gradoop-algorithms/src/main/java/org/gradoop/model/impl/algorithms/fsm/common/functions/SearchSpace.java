package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;

public class SearchSpace
  implements GroupReduceFunction<EdgeTriple, GSpanTransaction> {

  @Override
  public void reduce(Iterable<EdgeTriple> iterable,
    Collector<GSpanTransaction> collector) throws Exception {

    collector.collect(GSpan.createTransaction(iterable));
  }
}
