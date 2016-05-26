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

package org.gradoop.model.impl.algorithms.fsm.common;

import java.io.Serializable;

/**
 * frequent subgraph mining configuration
 */
public class FSMConfig implements Serializable {

  /**
   * Minimum relative support of a subgraph.
   * Subgraph containing above the threshold are considered to be frequent.
   */
  private final float threshold;

  /**
   * Direction mode, true for directed graphs and false for undirected.
   */
  private final boolean directed;

  /**
   * Edge mode, true for multigraphs and false for simple graphs.
   */
  private final boolean  multiGraph;

  /**
   * Maximum subgraph size by edge count.
   */
  private int maxEdgeCount;

  private int minEdgeCount;

  /**
   * valued constructor
   * @param threshold minimum relative support of a subgraph
   * @param directed direction mode
   * @param multiGraph multigraph mode
   */
  public FSMConfig(float threshold, boolean directed, boolean multiGraph) {
    this.threshold = threshold;
    this.directed = directed;
    this.multiGraph = multiGraph;
    this.maxEdgeCount = 0;
    this.minEdgeCount = 0;
  }

  public float getThreshold() {
    return threshold;
  }

  public boolean isMultigraph() {
    return multiGraph;
  }

  public boolean isDirected() {
    return directed;
  }

  public int getMaxEdgeCount() {
    return maxEdgeCount;
  }

  public void setMaxEdgeCount(int maxEdgeCount) {
    this.maxEdgeCount = maxEdgeCount;
  }

  public int getMinEdgeCount() {
    return minEdgeCount;
  }

  public void setMinEdgeCount(int minEdgeCount) {
    this.minEdgeCount = minEdgeCount;
  }

  /**
   * convenience factory method for directed multigraphs
   * @param threshold minimum relative support of a subgraph
   * @return configuration for directed multigraphs
   */
  public static FSMConfig forDirectedMultigraph(float threshold) {
    return new FSMConfig(threshold, true, true);
  }
}
