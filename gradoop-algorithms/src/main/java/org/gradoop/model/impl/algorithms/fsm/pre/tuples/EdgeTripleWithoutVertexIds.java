package org.gradoop.model.impl.algorithms.fsm.pre.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeTripleWithoutVertexIds
  extends Tuple4<GradoopId, Integer, Integer, Integer> {

  public EdgeTripleWithoutVertexIds() {

  }

  public EdgeTripleWithoutVertexIds(GradoopId graphId,
    Integer edgeLabel, Integer sourceLabel, Integer targetLabel) {
    super(graphId, edgeLabel, sourceLabel, targetLabel);

  }

  public Integer getEdgeLabel() {
    return f1;
  }

  public Integer getSourceLabel() {
    return f2;
  }

  public Integer getTargetLabel() {
    return f3;
  }

}
