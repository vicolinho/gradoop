package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos;

import java.io.Serializable;

public class GSpanEdge
  implements Serializable, Comparable<GSpanEdge> {

  private final int sourceId;
  private final int sourceLabel;
  private final int edgeId;
  private final int label;
  private final int targetId;
  private final int targetLabel;


  public GSpanEdge(
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

  private int getMinVertexLabel() {
    return sourceIsMinimumLabel() ? sourceLabel : targetLabel;
  }

  @Override
  public int compareTo(GSpanEdge other) {
    // min vertex label
    int comparison = this.getMinVertexLabel() - other.getMinVertexLabel();

    if (comparison == 0) {
      // same minimum vertex label

      if (this.isLoop() != other.isLoop()) {
        // loop is smaller
        if (this.isLoop()) {
          comparison = -1;
        } else {
          comparison = 1;
        }
      }

      if (comparison == 0) {
        // both loop or both no loop
        comparison = this.getSourceLabel() - other.getSourceLabel();

        if (comparison == 0) {
          // both start from minimum vertex label
          comparison = this.getLabel() - other.getLabel();

          if(comparison == 0) {
            // same edge label
            comparison = this.getTargetLabel() - other.getTargetLabel();
          }
        }
      }
    }

    return comparison;
  }

  @Override
  public String toString() {
    return "(" + getSourceId() + ":" + getSourceLabel() + ")-[" +
      getEdgeId() + ":" + getLabel() +
      "]->(" + getTargetId() + ":" + getTargetLabel() + ")";
  }

  public boolean sourceIsMinimumLabel() {
    return sourceLabel <= targetLabel;
  }
}
