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

package org.gradoop.datagen.fsmtransactions;

import java.io.Serializable;

/**
 * configuration for transactional FSM data generators
 */
public class FSMTransactionGeneratorConfig implements Serializable {
  /**
   * number of graphs to generate
   */
  private final int graphCount;
  /**
   * minimum number of vertices
   */
  private final int minVertexCount;
  /**
   * maximum number of vertices
   */
  private final int maxVertexCount;
  /**
   * distribution of vertex count among graphs
   */
  private final int vertexDistribution;
  /**
   * number of distinct vertex labels
   */
  private final int vertexLabelCount;
  /**
   * length of vertex labels
   */
  private final int vertexLabelSize;
  /**
   * minimum number of edges
   */
  private final int minEdgeCount;
  /**
   * maximum number of edges
   */
  private final int maxEdgeCount;
  /**
   * distribution of vertex count among graphs
   */
  private final int edgeDistribution;
  /**
   * number of distinct edge labels
   */
  private final int edgeLabelCount;
  /**
   * length of edge labels
   */
  private final int edgeLabelSize;

  /**
   * constructor
   * @param graphCount number of graphs to generate
   * @param minVertexCount minimum number of vertices
   * @param maxVertexCount maximum number of vertices
   * @param vertexLabelCount number of distinct vertex labels
   * @param vertexLabelSize length of vertex labels
   * @param minEdgeCount minimum number of edges
   * @param maxEdgeCount maximum number of edges
   * @param edgeLabelCount number of distinct edge labels
   * @param edgeLabelSize length of edge labels
   */
  public FSMTransactionGeneratorConfig(
    int graphCount,
    int minVertexCount, int maxVertexCount,
    int vertexLabelCount, int vertexLabelSize,
    int minEdgeCount, int maxEdgeCount,
    int edgeLabelCount, int edgeLabelSize
  ) {

    this.graphCount = graphCount;

    vertexDistribution = Distributions.LINEAR;

    this.minVertexCount = minVertexCount;
    this.maxVertexCount = maxVertexCount;

    this.vertexLabelCount = vertexLabelCount;
    this.vertexLabelSize = vertexLabelSize;

    this.edgeDistribution = Distributions.LINEAR;

    this.minEdgeCount = minEdgeCount;
    this.maxEdgeCount = maxEdgeCount;

    this.edgeLabelCount = edgeLabelCount;
    this.edgeLabelSize = edgeLabelSize;
  }

  /**
   * calculate number of vertices
   * @param x graph id
   * @param n number of graphs
   * @return number of vertices
   */
  public int calculateVertexCount(Long x, Long n) {
    return calculateCount(x, n,
      vertexDistribution, minVertexCount, maxVertexCount);
  }

  /**
   * calculate number of edges
   * @param x graph id
   * @param n number of graphs
   * @return number of edges
   */
  public int calculateEdgeCount(Long x, Long n) {
    return calculateCount(x, n,
      edgeDistribution, minEdgeCount, maxEdgeCount);
  }

  /**
   * calculate number of vertices or edges
   * @param x graph id
   * @param n number of graphs
   * @param distribution distribution function
   * @param min min number
   * @param max max number
   * @return number of vertices or edges
   */
  private int calculateCount(
    Long x, Long n, int distribution, int min, int max) {

    int count = 0;

    if (distribution == Distributions.LINEAR) {
      count = (int) (min + Math.round((double) x / n * (max - min)));
    }

    return count;
  }

  public long getGraphCount() {
    return graphCount;
  }

  public int getVertexLabelCount() {
    return vertexLabelCount;
  }

  public int getVertexLabelSize() {
    return vertexLabelSize;
  }

  public int getEdgeLabelCount() {
    return edgeLabelCount;
  }

  public int getEdgeLabelSize() {
    return edgeLabelSize;
  }

  /**
   * distribution enum
   */
  public static class Distributions {
    /**
     * linear distribution f(x) = ax + b
     */
    public static final int LINEAR = 0;
  }
}
