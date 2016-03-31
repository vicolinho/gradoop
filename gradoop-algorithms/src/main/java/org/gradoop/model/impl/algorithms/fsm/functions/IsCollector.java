package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;

public class IsCollector implements FilterFunction<SearchSpaceItem> {

  @Override
  public boolean filter(SearchSpaceItem searchSpaceItem) throws Exception {
    return searchSpaceItem.isCollector();
  }
}
