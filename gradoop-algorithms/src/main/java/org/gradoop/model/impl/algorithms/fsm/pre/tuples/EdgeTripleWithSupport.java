package org.gradoop.model.impl.algorithms.fsm.pre.tuples;

import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeTripleWithSupport
  extends
  Tuple7<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer, Integer> {

  public EdgeTripleWithSupport() {

  }

  public EdgeTripleWithSupport(
    GradoopId graphId, GradoopId sourceId, GradoopId targetId,
    Integer edgeLabel, Integer sourceLabel, Integer targetLabel,
    Integer support) {
    super(graphId, sourceId, targetId,
      edgeLabel, sourceLabel, targetLabel, support);
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

  public Integer getSupport() {
    return f6;
  }

}
