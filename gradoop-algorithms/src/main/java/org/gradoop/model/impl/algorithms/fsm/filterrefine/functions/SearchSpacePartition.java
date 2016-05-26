package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;

import java.util.HashMap;
import java.util.Map;

public class SearchSpacePartition extends RichMapPartitionFunction
  <GSpanTransaction, Tuple2<Integer, Map<Integer, GSpanTransaction>>> {

  @Override
  public void mapPartition(Iterable<GSpanTransaction> iterable,
    Collector<Tuple2<Integer, Map<Integer, GSpanTransaction>>> collector) throws Exception {
    int graphId = 0;

    Map<Integer, GSpanTransaction> graphs = new HashMap<>();

    for(GSpanTransaction graph : iterable) {
      graphs.put(graphId, graph);

      graphId++;
    }

    collector.collect(
      new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), graphs));
  }
}
