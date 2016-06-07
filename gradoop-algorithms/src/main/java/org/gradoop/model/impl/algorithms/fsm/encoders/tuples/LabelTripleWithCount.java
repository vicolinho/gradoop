package org.gradoop.model.impl.algorithms.fsm.encoders.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

public class LabelTripleWithCount
  extends Tuple4<Integer, Integer, Integer, Integer> {

  public LabelTripleWithCount() {
  }

  public LabelTripleWithCount(
    Integer edgeLabel, Integer sourceLabel, Integer targetLabel, int support) {
    super(edgeLabel, sourceLabel, targetLabel, support);
  }

  public int getEdgeLabel() {
    return f0;
  }

  public int getSourceLabel() {
    return f1;
  }

  public int getTargetLabel() {
    return f2;
  }

  public int getSupport() {
    return f3;
  }

}
