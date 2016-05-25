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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * pojo representing a mapping between a graphs vertices and edges and a DFS
 * code
 */
public class DFSEmbedding {
  /**
   * discovery times of vertices (index is time)
   */
  private final List<Integer> vertexTimes;
  /**
   * discovery times of edges (index is time)
   */
  private final List<Integer> edgeTimes;
  private final int minEdgePatternId;

  /**
   * constructor
   * @param minEdgePatternId
   * @param vertexTimes initial vertex discovery times
   * @param edgeTimes initial edge discovery times
   */
  public DFSEmbedding(
    int minEdgePatternId, List<Integer> vertexTimes, List<Integer> edgeTimes) {
    this.minEdgePatternId = minEdgePatternId;
    this.vertexTimes = vertexTimes;
    this.edgeTimes = edgeTimes;
  }

  public DFSEmbedding(List<Integer> vertexTimes, List<Integer> edgeTimes) {
    this.minEdgePatternId = 0;
    this.vertexTimes = vertexTimes;
    this.edgeTimes = edgeTimes;
  }

  @Override
  public String toString() {
    return "(tv=" + vertexTimes + ", " + "te=" + edgeTimes + ")";
  }

  public int getMinEdgePatternId() {
    return minEdgePatternId;
  }

  public List<Integer> getVertexTimes() {
    return vertexTimes;
  }

  public List<Integer> getEdgeTimes() {
    return edgeTimes;
  }

  /**
   * deep copy method
   * @param embedding input embedding of a DFS code
   * @return deep copy of the input
   */
  public static DFSEmbedding deepCopy(DFSEmbedding embedding) {
    return new DFSEmbedding(
      embedding.getMinEdgePatternId(),
      Lists.newArrayList(embedding.getVertexTimes()),
      Lists.newArrayList(embedding.getEdgeTimes())
    );
  }
}
