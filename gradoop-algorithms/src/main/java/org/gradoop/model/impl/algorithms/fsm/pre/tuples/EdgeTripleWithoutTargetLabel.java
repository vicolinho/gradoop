package org.gradoop.model.impl.algorithms.fsm.pre.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeTripleWithoutTargetLabel
  extends Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer> {

  public EdgeTripleWithoutTargetLabel() {
  }

  public EdgeTripleWithoutTargetLabel(GradoopId graphId, GradoopId sourceId,
    GradoopId targetId, Integer edgeLabel, Integer sourceLabel) {
    super(graphId, sourceId, targetId, edgeLabel, sourceLabel);

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

  public Integer getSourceLabel() {
    return f4;
  }


}
