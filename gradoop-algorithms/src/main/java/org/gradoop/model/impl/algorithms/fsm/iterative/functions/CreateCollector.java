package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;


public class CreateCollector implements MapFunction<Boolean, IterationItem> {
  @Override
  public IterationItem map(Boolean aBoolean) throws Exception {
    return new IterationItem();
  }
}
