package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos;

/**
 * Created by peet on 25.05.16.
 */
public class MinimalEdge {
  private final int sourceId;
  private final int sourceLabel;
  private final int label;
  private final int targetId;
  private final int targetLabel;

  public MinimalEdge(int sourceId, int sourceLabel, int label, int targetId,
    int targetLabel) {
    this.sourceId = sourceId;
    this.sourceLabel = sourceLabel;
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

  public int getLabel() {
    return label;
  }

  public int getTargetId() {
    return targetId;
  }

  public int getTargetLabel() {
    return targetLabel;
  }
}
