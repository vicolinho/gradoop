package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledVertex;
import org.gradoop.model.impl.id.GradoopId;


public class VertexLabelGraphId
  implements MapFunction
  <Tuple2<GradoopId, StringLabeledVertex>, Tuple3<String, GradoopId, Integer>> {

  @Override
  public Tuple3<String, GradoopId, Integer> map(
    Tuple2<GradoopId, StringLabeledVertex>  graphIdVertex) throws Exception {
    return new Tuple3<>(graphIdVertex.f1.getLabel(), graphIdVertex.f0, 1);
  }
}
