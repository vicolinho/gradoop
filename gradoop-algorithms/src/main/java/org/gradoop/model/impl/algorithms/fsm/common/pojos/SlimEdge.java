package org.gradoop.model.impl.algorithms.fsm.common.pojos;

import java.io.Serializable;

public class SlimEdge implements Serializable {

  private final int minId;
  private final int minLabel;
  private final int edgeId;
  private final boolean outgoing;
  private final int label;
  private final int maxId;
  private final int maxLabel;


  public SlimEdge(
    int minId, int minLabel,
    int edgeId, boolean outgoing, int label,
    int maxId, int maxLabel) {

    this.minId = minId;
    this.minLabel = minLabel;
    this.edgeId = edgeId;
    this.outgoing = outgoing;
    this.label = label;
    this.maxId = maxId;
    this.maxLabel = maxLabel;
  }

  public int getMinId() {
    return minId;
  }

  public int getMinLabel() {
    return minLabel;
  }

  public int getEdgeId() {
    return edgeId;
  }

  public boolean isOutgoing() {
    return outgoing;
  }

  public int getLabel() {
    return label;
  }

  public int getMaxId() {
    return maxId;
  }

  public int getMaxLabel() {
    return maxLabel;
  }

  public boolean isLoop() {
    return minId == maxId;
  }
}
