package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;

import java.util.HashMap;
import java.util.Map;


public class WorkerIdGraphCount
  implements GroupReduceFunction
  <Tuple2<Integer, Map<Integer, Transaction>>, Map<Integer, Integer>> {

  @Override
  public void reduce(
    Iterable<Tuple2<Integer, Map<Integer, Transaction>>> iterable,
    Collector<Map<Integer, Integer>> collector) throws Exception {

    Map<Integer, Integer> workerIdGraphCount = new HashMap<>();

    for (Tuple2<Integer, Map<Integer, Transaction>> pair : iterable) {
      workerIdGraphCount.put(pair.f0, pair.f1.size());
    }

    collector.collect(workerIdGraphCount);
  }
}
