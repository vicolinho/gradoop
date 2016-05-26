package org.gradoop.model.impl.algorithms.fsm.common.pojos;

import java.io.Serializable;

public class IntegerEdgeTriple implements Serializable {

  private final int sourceId;
  private final int sourceLabel;
  private final int edgeId;
  private final int label;
  private final int targetId;
  private final int targetLabel;


  public IntegerEdgeTriple(
    int sourceId, int sourceLabel,
    int edgeId, int label,
    int targetId, int targetLabel) {

    this.sourceId = sourceId;
    this.sourceLabel = sourceLabel;
    this.edgeId = edgeId;
    this.label = label;
    this.targetId = targetId;
    this.targetLabel = targetLabel;
  }

  public int getSourceId() {
    return sourceId;
  }

  public int getSourceLabel() {
    return sourceLabel;
  }

  public int getEdgeId() {
    return edgeId;
  }

  public int getLabel() {
    return label;
  }

  public int getTargetId() {
    return targetId;
  }

  public int getTargetLabel() {
    return targetLabel;
  }

  public boolean isLoop() {
    return sourceId == targetId;
  }
}
