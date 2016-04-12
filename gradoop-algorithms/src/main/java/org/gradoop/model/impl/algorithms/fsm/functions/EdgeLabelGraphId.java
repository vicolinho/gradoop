package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 12.04.16.
 */
public class EdgeLabelGraphId implements MapFunction
  <Tuple2<GradoopId, StringLabeledEdge>, Tuple3<String, GradoopId, Integer>> {

  @Override
  public Tuple3<String, GradoopId, Integer> map(
    Tuple2<GradoopId, StringLabeledEdge> graphIdEdge)
    throws
    Exception {
    return new Tuple3<>(graphIdEdge.f1.getLabel(), graphIdEdge.f0, 1);
  }
}
