package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Map;

public class WorkerIdsGraphCounts implements
  GroupReduceFunction<Tuple2<Integer, Integer>, Map<Integer, Integer>> {

  @Override
  public void reduce(Iterable<Tuple2<Integer, Integer>> iterable,
    Collector<Map<Integer, Integer>> collector) throws Exception {

    Map<Integer, Integer> workerIdGraphCount = Maps.newHashMap();

    for(Tuple2<Integer, Integer> pair : iterable) {
      if(pair.f1 > 0) {
        workerIdGraphCount.put(pair.f0, pair.f1);
      }
    }

    collector.collect(workerIdGraphCount);
  }
}
