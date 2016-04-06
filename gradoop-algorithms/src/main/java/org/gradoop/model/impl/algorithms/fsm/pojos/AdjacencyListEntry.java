/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.gradoop.model.impl.id.GradoopId;

/**
 * pojo representing an adjacency list entry
 */
public class AdjacencyListEntry {
  /**
   * true, if edge is outgoing
   */
  private final boolean outgoing;
  /**
   * edge id
   */
  private final GradoopId edgeId;
  /**
   * edge label
   */
  private final String edgeLabel;
  /**
   * vertexId
   */
  private final GradoopId vertexId;
  /**
   * vertex label
   */
  private final String vertexLabel;

  /**
   * constructor
   * @param outgoing true, if edge is outgoing
   * @param edgeId edge id
   * @param edgeLabel edge label
   * @param vertexId connected vertex id
   * @param vertexLabel connected vertex label
   */
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
      "-" + edgeLabel + ":" + edgeId + "-" +
      (outgoing ? ">" : "") +
      "(" + vertexLabel + ":" + vertexId + ")";
  }

  /**
   * factory method for outgoing adjacency list entries
   * @param edgeId edge id
   * @param edgeLabel edge label
   * @param targetId target vertex id
   * @param targetLabel target vertex label
   * @return entry
   */
  public static AdjacencyListEntry newOutgoing(GradoopId edgeId,
    String edgeLabel, GradoopId targetId, String targetLabel) {

    return new AdjacencyListEntry(
      true, edgeId, edgeLabel, targetId, targetLabel);
  }

  /**
   * factory method for incoming adjacency list entries
   * @param edgeId edge id
   * @param edgeLabel edge label
   * @param sourceId source vertex id
   * @param sourceLabel source vertex label
   * @return entry
   */
  public static AdjacencyListEntry newIncoming(GradoopId edgeId,
    String edgeLabel,  GradoopId sourceId, String sourceLabel) {

    return new AdjacencyListEntry(
      false, edgeId, edgeLabel, sourceId, sourceLabel);
  }
}
