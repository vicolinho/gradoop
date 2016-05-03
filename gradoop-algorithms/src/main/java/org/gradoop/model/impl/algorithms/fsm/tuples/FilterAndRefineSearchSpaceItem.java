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

package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import scala.collection.mutable.StringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * instances either represent graphs OR the collector for frequent DFS codes
 *
 * f0 : false                             true
 * f1 :              true, if active
 * adjacencyLists : map vertexId - adjacency list     empty map
 * codeEmbeddings : map DFS code - embeddings         empty map
 * f4 : empty array                       frequent DFS codes
 */
public class FilterAndRefineSearchSpaceItem implements Serializable {

  private ArrayList<AdjacencyList> adjacencyLists;
  private HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings;

  public FilterAndRefineSearchSpaceItem(ArrayList<AdjacencyList> adjacencyLists,
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings) {
    setAdjacencyLists(adjacencyLists);
    setCodeEmbeddings(codeEmbeddings);

  }

  public ArrayList<AdjacencyList> getAdjacencyLists() {
    return adjacencyLists;
  }

  public void setAdjacencyLists(
    ArrayList<AdjacencyList> adjacencyLists) {

    this.adjacencyLists = adjacencyLists;
  }

  public HashMap<CompressedDFSCode, HashSet<DFSEmbedding>>
  getCodeEmbeddings() {

    return codeEmbeddings;
  }

  public void setCodeEmbeddings(
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings) {

    this.codeEmbeddings = codeEmbeddings;
  }

  @Override
  public String toString() {

    StringBuilder builder = new StringBuilder("FilterAndRefineSearchSpaceItem");


    builder.append(" (Graph)\n\tAdjacency lists");

    int vertexIndex = 0;
    for (AdjacencyList entry : getAdjacencyLists()) {

      builder.append("\n\t\t(" + entry.getVertexLabel() + ":" +
        vertexIndex + ") : " +
        StringUtils.join(entry.getEntries(), " | "));

      vertexIndex++;
    }

    builder.append("\n\tDFS codes and embeddings");

    for (Map.Entry<CompressedDFSCode, HashSet<DFSEmbedding>> entry :
      getCodeEmbeddings().entrySet()) {

      builder.append("\n\t\t" + entry.getKey().getDfsCode());

      for (DFSEmbedding embedding : entry.getValue()) {
        builder.append(embedding);
      }
    }

    return builder.toString();
  }
}
