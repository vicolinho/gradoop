package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;

import java.util.HashMap;
import java.util.Map;

public class SearchSpacePartition extends RichMapPartitionFunction
  <Transaction, Tuple2<Integer, Map<Integer, Transaction>>> {

  @Override
  public void mapPartition(Iterable<Transaction> iterable,
    Collector<Tuple2<Integer, Map<Integer, Transaction>>> collector) throws Exception {
    int graphId = 0;

    Map<Integer, Transaction> graphs = new HashMap<>();

    for(Transaction graph : iterable) {
      graphs.put(graphId, graph);

      graphId++;
    }

    collector.collect(
      new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), graphs));
  }
}
