package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;

import java.util.ArrayList;
import java.util.List;

public class ConcatFrequentDfsCodes implements GroupReduceFunction
  <Tuple2<CompressedDfsCode, Integer>, CompressedDfsCode[]> {

  @Override
  public void reduce(Iterable<Tuple2<CompressedDfsCode, Integer>> iterable,
    Collector<CompressedDfsCode[]> collector) throws
    Exception {

    List<CompressedDfsCode> codes = new ArrayList<>();

    for(Tuple2<CompressedDfsCode, Integer> pair : iterable) {
      codes.add(pair.f0);
    }

    collector.collect(codes.toArray(new CompressedDfsCode[codes.size()]));
  }
}
