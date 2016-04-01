package org.gradoop.model.impl.algorithms.fsm.pojos;

public class EdgePattern {

  private final String minVertexLabel;
  private final Boolean outgoing;
  private final String edgeLabel;
  private final String maxVertexLabel;

  public EdgePattern(String fromVertexLabel,
    boolean outgoing, String edgeLabel, String maxVertexLabel) {

    Boolean minToMax = fromVertexLabel.compareTo(maxVertexLabel) <= 0;

    this.minVertexLabel = minToMax ? fromVertexLabel : maxVertexLabel;
    this.outgoing = minToMax == outgoing;
    this.edgeLabel = edgeLabel;
    this.maxVertexLabel = minToMax ? maxVertexLabel : fromVertexLabel;
  }

  public String getMinVertexLabel() {
    return minVertexLabel;
  }

  public Boolean getOutgoing() {
    return outgoing;
  }

  public String getEdgeLabel() {
    return edgeLabel;
  }

  public String getMaxVertexLabel() {
    return maxVertexLabel;
  }
}
