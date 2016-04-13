package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.id.GradoopId;


public class VertexLabelGraphId<L extends Comparable<L>>
  implements MapFunction
  <Tuple2<GradoopId, LabeledVertex<L>>, Tuple3<L, GradoopId, Integer>> {

  @Override
  public Tuple3<L, GradoopId, Integer> map(
    Tuple2<GradoopId, LabeledVertex<L>>  graphIdVertex) throws Exception {
    return new Tuple3<>(graphIdVertex.f1.getLabel(), graphIdVertex.f0, 1);
  }
}
