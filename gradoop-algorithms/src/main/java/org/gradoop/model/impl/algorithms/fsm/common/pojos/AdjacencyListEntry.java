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

package org.gradoop.model.impl.algorithms.fsm.common.pojos;

/**
 * pojo representing an adjacency list entry
 */
public class AdjacencyListEntry {
  private final int minEdgePatternId;

  /**
   * true, if edge is outgoing
   */
  private final boolean outgoing;
  /**
   * edge id
   */
  private final Integer edgeId;
  /**
   * edge label
   */
  private final Integer edgeLabel;
  /**
   * vertexId
   */
  private final Integer vertexId;
  /**
   * vertex label
   */
  private final Integer vertexLabel;

  /**
   * constructor
   * @param minEdgePatternId
   * @param outgoing true, if edge is outgoing
   * @param edgeId edge id
   * @param edgeLabel edge label
   * @param vertexId connected vertex id
   * @param vertexLabel connected vertex label
   */
  public AdjacencyListEntry(int minEdgePatternId, boolean outgoing, Integer edgeId,
    Integer edgeLabel, Integer vertexId, Integer vertexLabel) {

    this.minEdgePatternId = minEdgePatternId;
    this.outgoing = outgoing;
    this.edgeId = edgeId;
    this.edgeLabel = edgeLabel;
    this.vertexId = vertexId;
    this.vertexLabel = vertexLabel;
  }

  public int getMinEdgePatternId() {
    return minEdgePatternId;
  }

  public Integer getVertexLabel() {
    return vertexLabel;
  }

  public boolean isOutgoing() {
    return outgoing;
  }

  public Integer getEdgeId() {
    return edgeId;
  }

  public Integer getEdgeLabel() {
    return edgeLabel;
  }

  public Integer getVertexId() {
    return vertexId;
  }

  @Override
  public String toString() {
    return (outgoing ? "" : "<") +
      "-" + edgeId + ":" + edgeLabel + "-" +
      (outgoing ? ">" : "") +
      "(" + vertexId + ":" + vertexLabel + ")";
  }
}
