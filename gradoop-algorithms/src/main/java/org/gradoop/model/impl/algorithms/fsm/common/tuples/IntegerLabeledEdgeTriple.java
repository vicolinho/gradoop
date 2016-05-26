package org.gradoop.model.impl.algorithms.fsm.common.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;

public class IntegerLabeledEdgeTriple
  extends Tuple5<GradoopId, GradoopId, Integer, Integer, Integer> {

  public IntegerLabeledEdgeTriple() {

  }

  public IntegerLabeledEdgeTriple(
    GradoopId minId, Integer minLabel,
    Integer label,
    GradoopId maxId, Integer maxLabel) {

    super(minId, maxId, label, minLabel, maxLabel);
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
