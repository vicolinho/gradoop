package org.gradoop.model.impl.algorithms.fsm.common.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;

public class FatEdge
  extends Tuple5<GradoopId, GradoopId, Integer, Integer, Integer> {

  public FatEdge() {

  }

  public FatEdge(GradoopId sourceId, GradoopId targetId, Integer edgeLabel,
    Integer sourceLabel, Integer targetLabel) {

    super(sourceId, targetId, edgeLabel, sourceLabel, targetLabel);
  }

  public GradoopId getSourceId() {
    return this.f0;
  }

  public GradoopId getTargetId() {
    return this.f1;
  }

  public Integer getLabel() {
    return f2;
  }

  public Integer getSourceLabel() {
    return f3;
  }

  public Integer getTargetLabel() {
    return f4;
  }
}
