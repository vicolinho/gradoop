package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 30.03.16.
 */
public class AdjacencyListEntry {
  private final boolean outgoing;
  private final GradoopId edgeId;
  private final String edgeLabel;
  private final GradoopId vertexId;
  private final String vertexLabel;

  public static AdjacencyListEntry newOutgoing(GradoopId edgeId,
    String edgeLabel, GradoopId targetId, String targetLabel) {

    return new AdjacencyListEntry(
      true, edgeId, edgeLabel, targetId, targetLabel);
  }

  public static AdjacencyListEntry newIncoming(GradoopId edgeId,
    String edgeLabel,  GradoopId sourceId, String sourceLabel) {

    return new AdjacencyListEntry(
      false, edgeId, edgeLabel, sourceId, sourceLabel);
  }


  public AdjacencyListEntry(boolean outgoing, GradoopId edgeId,
    String edgeLabel, GradoopId vertexId, String vertexLabel) {

    this.outgoing = outgoing;
    this.edgeId = edgeId;
    this.edgeLabel = edgeLabel;
    this.vertexId = vertexId;
    this.vertexLabel = vertexLabel;
  }

  public String getVertexLabel() {
    return vertexLabel;
  }

  public boolean isOutgoing() {
    return outgoing;
  }

  public GradoopId getEdgeId() {
    return edgeId;
  }

  public String getEdgeLabel() {
    return edgeLabel;
  }

  public GradoopId getVertexId() {
    return vertexId;
  }

  @Override
  public String toString() {
    return (outgoing ? "" : "<") +
      "-[" + edgeLabel + "]-" +
      (outgoing ? ">" : "") +
      vertexLabel;
  }
}
