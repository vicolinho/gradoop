package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;

public class StoreSupport implements
  MapFunction<Tuple2<CompressedDfsCode, Integer>, Tuple2<CompressedDfsCode, Integer>> {

  @Override
  public Tuple2<CompressedDfsCode, Integer> map(
    Tuple2<CompressedDfsCode, Integer> pair) throws
    Exception {

    pair.f0.setSupport(pair.f1);
    pair.f1 = 0;

    return pair;
  }
}
