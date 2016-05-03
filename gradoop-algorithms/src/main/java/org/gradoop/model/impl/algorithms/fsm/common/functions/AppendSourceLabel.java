package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.VertexIdLabel;
import org.gradoop.model.impl.id.GradoopId;

public class AppendSourceLabel
  implements JoinFunction<
  Tuple4<GradoopId, GradoopId, GradoopId, Integer>, VertexIdLabel,
  Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer>
  > {

  @Override
  public Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer> join(
    Tuple4<GradoopId, GradoopId, GradoopId, Integer> edge,
    VertexIdLabel sourceVertex) throws Exception {
    return new Tuple5<>(edge.f0, edge.f1, edge.f2, edge.f3, sourceVertex.f1);
  }
}
