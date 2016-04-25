package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;

public class AppendSourceLabel
  implements JoinFunction<
  Tuple4<GradoopId, GradoopId, GradoopId, Integer>,
  Tuple2<GradoopId, Integer>,
  Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer>
  > {

  @Override
  public Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer> join(
    Tuple4<GradoopId, GradoopId, GradoopId, Integer> edge,
    Tuple2<GradoopId, Integer> sourceVertex) throws Exception {
    return new Tuple5<>(edge.f0, edge.f1, edge.f2, sourceVertex.f1, edge.f3);
  }
}
