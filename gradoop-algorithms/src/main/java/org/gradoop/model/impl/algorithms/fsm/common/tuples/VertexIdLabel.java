package org.gradoop.model.impl.algorithms.fsm.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 27.04.16.
 */
public class VertexIdLabel extends Tuple2<GradoopId, Integer> {

  public VertexIdLabel() {

  }

  public VertexIdLabel(GradoopId vertexId, Integer label) {
    super(vertexId, label);
  }
}
