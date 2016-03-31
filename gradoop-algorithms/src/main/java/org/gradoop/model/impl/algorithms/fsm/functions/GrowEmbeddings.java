package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;

public class GrowEmbeddings
  extends RichMapFunction<SearchSpaceItem, SearchSpaceItem> {


  public static final String DS_NAME = "DFSs";

  @Override
  public SearchSpaceItem map(SearchSpaceItem searchSpaceItem) throws Exception {
    return searchSpaceItem;
  }
}
