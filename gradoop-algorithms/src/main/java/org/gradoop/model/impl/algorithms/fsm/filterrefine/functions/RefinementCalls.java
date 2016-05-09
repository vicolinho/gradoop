package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.util.Collection;

public class RefinementCalls implements
  GroupReduceFunction<Tuple3<CompressedDFSCode, Integer, Boolean>, Tuple2<Integer, Collection<CompressedDFSCode>>> {

  @Override
  public void reduce(
    Iterable<Tuple3<CompressedDFSCode, Integer, Boolean>> iterable,
    Collector<Tuple2<Integer, Collection<CompressedDFSCode>>> collector) throws
    Exception {

    boolean first = true;
    Integer workerId = null;

    Collection<CompressedDFSCode> codes = Lists.newArrayList();

    for(Tuple3<CompressedDFSCode, Integer, Boolean> triple : iterable) {
      if (first) {
        workerId = triple.f1;
        first = false;
      }

      codes.add(triple.f0);
    }

    Tuple2<Integer, Collection<CompressedDFSCode>> call =
      new Tuple2<>(workerId, codes);

    System.out.println(call);

    collector.collect(call);
  }

}
