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
import org.apache.commons.lang3.StringUtils;
import scala.collection.mutable.StringBuilder;

import java.util.ArrayList;


public class AdjacencyLists {

  private final ArrayList<AdjacencyList> adjacencyLists;

  /**
   * default constructor
   */
  public AdjacencyLists() {
    adjacencyLists = Lists.newArrayList();
  }

  /**
   * valued constructor
   * @param adjacencyLists adjacency lists (empty for collector)
   */
  public AdjacencyLists(ArrayList<AdjacencyList> adjacencyLists) {
    this.adjacencyLists = adjacencyLists;
  }

  public ArrayList<AdjacencyList> getAdjacencyLists() {
    return adjacencyLists;
  }


  @Override
  public String toString() {

    StringBuilder builder = new StringBuilder("Adjacency lists");

    int vertexIndex = 0;
    for (AdjacencyList entry : getAdjacencyLists()) {

      builder.append("\n(" + vertexIndex + ":" +
        entry.getVertexLabel() + ") : " +
        StringUtils.join(entry.getEntries(), " | "));

      vertexIndex++;
    }

    return builder.toString();
  }
}
