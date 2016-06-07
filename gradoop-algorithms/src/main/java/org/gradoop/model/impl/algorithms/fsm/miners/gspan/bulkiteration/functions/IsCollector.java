package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.tuples.IterationItem;


public class IsCollector implements FilterFunction<IterationItem> {


  public IsCollector() {

  }

  @Override
  public boolean filter(IterationItem iterationItem) throws Exception {
    return iterationItem.isCollector();
  }
}
