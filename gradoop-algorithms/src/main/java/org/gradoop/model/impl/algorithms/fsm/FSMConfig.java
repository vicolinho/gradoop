package org.gradoop.model.impl.algorithms.fsm;

import java.io.Serializable;

public class FSMConfig implements Serializable {
  float threshold;
  private final boolean  directed;
  private final boolean  multiGraph;
  private boolean multigraph;

  public FSMConfig(float threshold, boolean directed, boolean multiGraph) {
    this.threshold = threshold;
    this.directed = directed;
    this.multiGraph = multiGraph;
  }

  public float getThreshold() {
    return threshold;
  }

  public static FSMConfig forDirectedMultigraph(float threshold) {
    return new FSMConfig(threshold, true, true);
  }

  public boolean isMultigraph() {
    return multigraph;
  }

  public boolean isDirected() {
    return directed;
  }
}
