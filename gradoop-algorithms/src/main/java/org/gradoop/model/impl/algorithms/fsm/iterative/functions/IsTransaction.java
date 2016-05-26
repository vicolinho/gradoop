package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

/**
 * Created by peet on 26.05.16.
 */
public class IsTransaction implements FilterFunction<IterationItem> {
  @Override
  public boolean filter(IterationItem iterationItem) throws Exception {
    return iterationItem.isTransaction();
  }
}
