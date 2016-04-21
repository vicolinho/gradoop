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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Collection;

/**
 * configuration for transactional FSM data generators
 */
public class FSMTransactionGeneratorConfig implements Serializable {
  /**
   * number of graphs to generate
   */
  private int graphCount;
  /**
   * minimum number of vertices
   */
  private int minVertexCount;
  /**
   * maximum number of vertices
   */
  private int maxVertexCount;
  /**
   * distribution of vertex count among graphs
   */
  private int vertexDistribution;
  /**
   * number of distinct vertex labels
   */
  private int vertexLabelCount;
  /**
   * length of vertex labels
   */
  private int vertexLabelSize;
  /**
   * minimum number of edges
   */
  private int minEdgeCount;
  /**
   * maximum number of edges
   */
  private int maxEdgeCount;
  /**
   * distribution of vertex count among graphs
   */
  private int edgeDistribution;
  /**
   * number of distinct edge labels
   */
  private int edgeLabelCount;
  /**
   * length of edge labels
   */
  private int edgeLabelSize;

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

  public void setGraphCount(int graphCount) {
    this.graphCount = graphCount;
  }

  public void setMinVertexCount(int minVertexCount) {
    this.minVertexCount = minVertexCount;
  }

  public void setMaxVertexCount(int maxVertexCount) {
    this.maxVertexCount = maxVertexCount;
  }

  public void setVertexDistribution(int vertexDistribution) {
    this.vertexDistribution = vertexDistribution;
  }

  public void setVertexLabelCount(int vertexLabelCount) {
    this.vertexLabelCount = vertexLabelCount;
  }

  public void setVertexLabelSize(int vertexLabelSize) {
    this.vertexLabelSize = vertexLabelSize;
  }

  public void setMinEdgeCount(int minEdgeCount) {
    this.minEdgeCount = minEdgeCount;
  }

  public void setMaxEdgeCount(int maxEdgeCount) {
    this.maxEdgeCount = maxEdgeCount;
  }

  public void setEdgeDistribution(int edgeDistribution) {
    this.edgeDistribution = edgeDistribution;
  }

  public void setEdgeLabelCount(int edgeLabelCount) {
    this.edgeLabelCount = edgeLabelCount;
  }

  public void setEdgeLabelSize(int edgeLabelSize) {
    this.edgeLabelSize = edgeLabelSize;
  }

  @Override
  public String toString() {
    Collection parameters = Lists.newArrayList(
      graphCount,
      minVertexCount,
      maxVertexCount,
      minEdgeCount,
      maxEdgeCount,
      vertexLabelCount,
      edgeLabelCount,
      vertexLabelSize,
      edgeLabelSize
    );

    return StringUtils.join(parameters, ";");
  }

  /**
   * distribution enum
   */
  public static class Distributions {
    /**
     * linear distribution f(x) = ax + b
     */
    public static int LINEAR = 0;
  }


}
