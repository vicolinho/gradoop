package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

public class AppendTargetLabel implements JoinFunction<
  Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer>,
  Tuple2<GradoopId, Integer>,
  Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>
  > {

  @Override
  public Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>
  join(Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer> edge,
    Tuple2<GradoopId, Integer> targetVertex) throws Exception {

    return new Tuple6<>(
      edge.f0, edge.f1, edge.f2, edge.f3, edge.f4, targetVertex.f1);
  }
}
