package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

public class CountableTriplePattern implements MapFunction
  <Tuple4<GradoopId, Integer, Integer, Integer>,
    Tuple4<Integer, Integer, Integer, Integer>> {

  @Override
  public Tuple4<Integer, Integer, Integer, Integer> map(
    Tuple4<GradoopId, Integer, Integer, Integer> graphPattern) throws
    Exception {

    return new Tuple4<>(graphPattern.f1, graphPattern.f2, graphPattern.f3, 1);
  }
}
