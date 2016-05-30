package org.gradoop.model.impl.algorithms.fsm.pre.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by peet on 30.05.16.
 */
public class LabelTripleWithSupport
  extends Tuple4<Integer, Integer, Integer, Integer> {

  public LabelTripleWithSupport(
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
