package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

import java.util.Collection;

public class RefinementCalls implements
  GroupReduceFunction<Tuple3<CompressedDfsCode, Integer, Boolean>, Tuple2<Integer, Collection<CompressedDfsCode>>> {

  @Override
  public void reduce(
    Iterable<Tuple3<CompressedDfsCode, Integer, Boolean>> iterable,
    Collector<Tuple2<Integer, Collection<CompressedDfsCode>>> collector) throws
    Exception {

    boolean first = true;
    Integer workerId = null;

    Collection<CompressedDfsCode> codes = Lists.newArrayList();

    for(Tuple3<CompressedDfsCode, Integer, Boolean> triple : iterable) {
      if (first) {
        workerId = triple.f1;
        first = false;
      }

      codes.add(triple.f0);
    }

    Tuple2<Integer, Collection<CompressedDfsCode>> call =
      new Tuple2<>(workerId, codes);

    collector.collect(call);
  }

}
