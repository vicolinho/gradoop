package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpacePartition;

public class LocalFSM implements
  MapFunction<SearchSpacePartition, SearchSpacePartition> {
  public static final String DS_NAME = "minSupport";

  @Override
  public SearchSpacePartition map(
    SearchSpacePartition searchSpacePartition) throws Exception {
    return null;
  }
}
