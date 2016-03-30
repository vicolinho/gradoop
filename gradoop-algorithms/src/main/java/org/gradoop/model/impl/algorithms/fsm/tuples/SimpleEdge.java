package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

public class SimpleEdge extends
  Tuple4<GradoopId, GradoopId, GradoopId, String> {

  public SimpleEdge(){}

  public SimpleEdge(
    GradoopId edgeId, GradoopId sourceId, GradoopId targetId, String label) {
    this.f0 = edgeId;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public GradoopId getTargetId() {
    return this.f2;
  }

  public String getLabel() {
    return this.f3;
  }
}
