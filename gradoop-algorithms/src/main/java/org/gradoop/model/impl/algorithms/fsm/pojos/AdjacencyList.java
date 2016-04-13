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

import java.util.ArrayList;
import java.util.Collection;

/**
 * pojo representing an adjacency list
 */
public class AdjacencyList<L extends Comparable<L>> {
  /**
   * label of the associated vertex
   */
  private final L vertexLabel;
  /**
   * adjacency list entries
   */
  private final Collection<AdjacencyListEntry<L>> entries;

  /**
   * constructor
   * @param vertexLabel vertex label
   */
  public AdjacencyList(L vertexLabel) {
    this.vertexLabel = vertexLabel;
    entries = new ArrayList<>();
  }

  @Override
  public String toString() {
    return vertexLabel + ":" + entries;
  }

  public L getVertexLabel() {
    return vertexLabel;
  }

  public Collection<AdjacencyListEntry<L>> getEntries() {
    return entries;
  }
}
