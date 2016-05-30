package org.gradoop.model.impl.algorithms.fsm.pre.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeTriple
  extends Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer> {

  public EdgeTriple() {

  }

  public EdgeTriple(GradoopId graphId, GradoopId sourceId, GradoopId targetId,
    Integer edgeLabel, Integer sourceLabel, Integer targetLabel) {
    super(graphId, sourceId, targetId, edgeLabel, sourceLabel, targetLabel);

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

  public Integer getTargetLabel() {
    return f5;
  }

}
