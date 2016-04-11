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

package org.gradoop.model.impl.operators.tostring.pojos;

/**
 * pojo representing an edge pattern
 */
public class EdgePattern {

  /**
   * smaller of both vertex labels
   */
  private final String minVertexLabel;
  /**
   * true, smaller label is source label and edge represent edge in direction,
   * false, otherwise
   */
  private final Boolean minToMaxInDirection;
  /**
   * edge label
   */
  private final String edgeLabel;
  /**
   * larger of both vertes labels
   */
  private final String maxVertexLabel;

  /**
   * constructor
   * @param fromVertexLabel traversal start vertex label
   * @param outgoing true, if traversal in direction
   * @param edgeLabel edge label
   * @param maxVertexLabel traversal end vertex label
   */
  public EdgePattern(String fromVertexLabel,
    boolean outgoing, String edgeLabel, String maxVertexLabel) {

    Boolean minToMax = fromVertexLabel.compareTo(maxVertexLabel) <= 0;

    this.minVertexLabel = minToMax ? fromVertexLabel : maxVertexLabel;
    this.minToMaxInDirection = minToMax == outgoing;
    this.edgeLabel = edgeLabel;
    this.maxVertexLabel = minToMax ? maxVertexLabel : fromVertexLabel;
  }

  public String getMinVertexLabel() {
    return minVertexLabel;
  }

  public Boolean isOutgoing() {
    return minToMaxInDirection;
  }

  public String getEdgeLabel() {
    return edgeLabel;
  }

  public String getMaxVertexLabel() {
    return maxVertexLabel;
  }
}
