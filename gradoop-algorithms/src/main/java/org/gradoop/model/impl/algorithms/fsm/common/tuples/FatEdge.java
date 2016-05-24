package org.gradoop.model.impl.algorithms.fsm.common.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

public class FatEdge
  extends Tuple6<GradoopId, GradoopId, Integer, Integer, Integer, Boolean> {

  public FatEdge() {

  }

  public FatEdge(GradoopId minId, Integer minLabel, Boolean outgoing,
    Integer label, GradoopId maxId, Integer maxLabel) {

    super(minId, maxId, label, minLabel, maxLabel, outgoing);
  }

  public GradoopId getMinId() {
    return this.f0;
  }

  public GradoopId getMaxId() {
    return this.f1;
  }

  public Integer getLabel() {
    return f2;
  }

  public Integer getMinLabel() {
    return f3;
  }

  public Integer getMaxLabel() {
    return f4;
  }

  public boolean isOutgoing() {
    return f5;
  }
}
