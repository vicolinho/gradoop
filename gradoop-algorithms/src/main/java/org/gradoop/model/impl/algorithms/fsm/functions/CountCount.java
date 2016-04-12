package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by peet on 12.04.16.
 */
public class CountCount implements
  MapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>> {

  @Override
  public Tuple2<Integer, Integer> map(
    Tuple2<String, Integer> labelCount) throws Exception {
    return new Tuple2<>(labelCount.f1, 1);
  }
}
