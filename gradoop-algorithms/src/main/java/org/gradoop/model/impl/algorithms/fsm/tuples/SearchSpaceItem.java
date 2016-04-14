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
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import scala.collection.mutable.StringBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * instances either represent graphs OR the collector for frequent DFS codes
 *
 * f0 : false                             true
 * f1 :              true, if active
 * f2 : map vertexId - adjacency list     empty map
 * f3 : map DFS code - embeddings         empty map
 * f4 : empty array                       frequent DFS codes
 */
public class SearchSpaceItem extends Tuple5<
    Boolean,
    Boolean,
    ArrayList<AdjacencyList>,
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>>,
    ArrayList<CompressedDFSCode>
  > {

  /**
   * default constructor
   */
  public SearchSpaceItem() {
  }

  /**
   * valued constructor
   * @param collector true for collector, false for graph
   * @param active true for active, false for inactive
   * @param adjacencyLists adjacency lists (empty for collector)
   * @param codeEmbeddings embeddings of DFS codes (empty for collector)
   * @param frequentDfsCodes frequent DFS codes (empty for graph)
   */
  public SearchSpaceItem(boolean collector, boolean active,
    ArrayList<AdjacencyList> adjacencyLists,
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings,
    ArrayList<CompressedDFSCode> frequentDfsCodes) {

    setCollector(collector);
    setActive(active);
    setAdjacencyLists(adjacencyLists);
    setCodeEmbeddings(codeEmbeddings);
    setFrequentDfsCodes(frequentDfsCodes);
  }

  /**
   * setter search space item to active or inactive.
   * If set to inactive, graph data will be dropped to release memory.
   * @param active true for active, false for inactive
   */
  public void setActive(Boolean active) {
    this.f1 = active;

    if (! active && ! isCollector()) {
      getCodeEmbeddings().clear();
      getAdjacencyLists().clear();
    }
  }

  public Boolean isCollector() {
    return this.f0;
  }

  public void setCollector(Boolean collector) {
    this.f0 = collector;
  }

  public Boolean isActive() {
    return this.f1;
  }

  public ArrayList<AdjacencyList> getAdjacencyLists() {
    return f2;
  }

  public void setAdjacencyLists(
    ArrayList<AdjacencyList> adjacencyLists) {

    this.f2 = adjacencyLists;
  }

  public HashMap<CompressedDFSCode, HashSet<DFSEmbedding>>
  getCodeEmbeddings() {
    return f3;
  }

  public void setCodeEmbeddings(
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings) {

//    System.out.println(getGraphId() + " updated embeddings to " +
//      codeEmbeddings.keySet());

    this.f3 = codeEmbeddings;
  }

  public ArrayList<CompressedDFSCode> getFrequentDfsCodes() {
    return this.f4;
  }

  public void setFrequentDfsCodes(
    ArrayList<CompressedDFSCode> collectedDfsCodes) {
    this.f4 = collectedDfsCodes;
  }

  @Override
  public String toString() {

    StringBuilder builder = new StringBuilder("SearchSpaceItem");

    if (isCollector()) {
      builder.append(" (Collector)\n\tFrequent DFS codes\n");

      for (CompressedDFSCode compressedDfsCode : getFrequentDfsCodes()) {
        builder.append("\n" + compressedDfsCode);
      }
    } else {
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
    }

    return builder.toString();
  }

  /**
   * factory method
   * @param adjacencyLists adjacency lists
   * @param codeEmbeddings embeddings of DFS codes
   * @return a search space item representing a graph transaction
   */
  public static SearchSpaceItem createForGraph(
    ArrayList<AdjacencyList> adjacencyLists,
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings) {

    return new SearchSpaceItem(false, true, adjacencyLists, codeEmbeddings,
      new ArrayList<CompressedDFSCode>());
  }

  /**
   * factory method
   * @return a search space item representing the collector
   */
  public static SearchSpaceItem createCollector() {
    ArrayList<AdjacencyList> adjacencyLists = new ArrayList<>();
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings = new
      HashMap<>();

    return new SearchSpaceItem(true, true, adjacencyLists, codeEmbeddings,
      new ArrayList<CompressedDFSCode>());
  }
}
