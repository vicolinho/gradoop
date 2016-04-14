package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

public class CountableLabel implements
  MapFunction<Tuple2<GradoopId, String>, Tuple2<String, Integer>> {
  @Override
  public Tuple2<String, Integer> map(
    Tuple2<GradoopId, String> gidVidLabel) throws
    Exception {
    return new Tuple2<>(gidVidLabel.f1, 1);
  }
}
