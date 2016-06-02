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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class GSpanTransaction implements Serializable {

  private List<AdjacencyList> adjacencyLists;
  private Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings;

  public GSpanTransaction(
    List<AdjacencyList> adjacencyLists,
    Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings) {

    this.adjacencyLists = adjacencyLists;
    this.codeEmbeddings = codeEmbeddings;
  }

  public List<AdjacencyList> getAdjacencyLists() {
    return adjacencyLists;
  }

  public Map<DfsCode, Collection<DFSEmbedding>>
  getCodeEmbeddings() {

    return codeEmbeddings;
  }

  public void setCodeEmbeddings(
    Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings) {

    this.codeEmbeddings = codeEmbeddings;
  }

  public Boolean hasGrownSubgraphs() {
    return this.codeEmbeddings != null;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    int listId = 0;

    for (AdjacencyList adjacencyList : adjacencyLists) {
      builder.append("(" + listId + ":" + adjacencyList.getFromVertexLabel()+
        ")" + adjacencyList.getEntries() + "\n");
      listId++;
    }

    return builder.toString();
  }
}
