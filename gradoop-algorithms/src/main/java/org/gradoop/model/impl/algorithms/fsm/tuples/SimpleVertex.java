package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

public class SimpleVertex extends Tuple2<GradoopId, String> {

  public SimpleVertex(){}

  public SimpleVertex(GradoopId vertexId, String label) {
    this.f0 = vertexId;
    this.f1 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public String getLabel() {
    return this.f1;
  }
}
