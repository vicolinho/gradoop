package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpacePartition;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;

/**
 * Created by peet on 22.04.16.
 */
public class LocalDfsCodeSupports implements
  FlatMapFunction<SearchSpacePartition, CompressedDFSCode> {

  @Override
  public void flatMap(SearchSpacePartition searchSpacePartition,
    Collector<CompressedDFSCode> collector) throws Exception {

  }
}
