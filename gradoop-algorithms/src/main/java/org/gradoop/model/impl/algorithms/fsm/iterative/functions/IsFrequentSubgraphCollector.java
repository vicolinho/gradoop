package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;


public class IsFrequentSubgraphCollector implements FilterFunction<IterationItem> {

  private final boolean lastIteration;

  public IsFrequentSubgraphCollector(boolean lastIteration) {
    this.lastIteration = lastIteration;
  }

  @Override
  public boolean filter(IterationItem iterationItem) throws Exception {
    return iterationItem.isCollector() &&
      lastIteration == iterationItem.isLastIteration();
  }
}
