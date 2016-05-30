package org.gradoop.model.impl.algorithms.fsm.pre.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

public class VertexIdLabel extends Tuple2<GradoopId, Integer> {

  public VertexIdLabel() {

  }

  public VertexIdLabel(GradoopId vertexId, Integer label) {
    super(vertexId, label);
  }

  public Integer getLabel() {
    return this.f1;
  }
}
