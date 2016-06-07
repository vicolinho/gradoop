package org.gradoop.model.impl.algorithms.fsm.encoders.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeTripleWithoutVertexLabels
  extends Tuple4<GradoopId, GradoopId, GradoopId, Integer> {

  public EdgeTripleWithoutVertexLabels() {

  }

  public EdgeTripleWithoutVertexLabels(
    GradoopId graphId, GradoopId sourceId, GradoopId targetId, Integer edgeLabel) {
    super(graphId, sourceId, targetId, edgeLabel);

  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public GradoopId getTargetId() {
    return this.f2;
  }

  public Integer getEdgeLabel() {
    return f3;
  }
}
