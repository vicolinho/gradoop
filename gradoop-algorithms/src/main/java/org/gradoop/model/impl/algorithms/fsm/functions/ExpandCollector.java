package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;

public class ExpandCollector implements
  FlatMapFunction<SearchSpaceItem, CompressedDfsCode> {

  @Override
  public void flatMap(SearchSpaceItem searchSpaceItem,
    Collector<CompressedDfsCode> collector) throws Exception {

    for(CompressedDfsCode compressedDfsCode :
      searchSpaceItem.getCollectedDfsCodes()) {

      collector.collect(compressedDfsCode);
    }

  }
}
