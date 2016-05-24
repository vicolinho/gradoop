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

package org.gradoop.model.impl.algorithms.fsm.common.tuples;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import scala.collection.mutable.StringBuilder;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public class Transaction implements Serializable {

  private Map<Integer, AdjacencyList> adjacencyLists;
  private Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings;
  private Collection<Collection<DFSCode>> codeSiblings;

  public Transaction(
    Map<Integer, AdjacencyList> adjacencyLists,
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings,
    Collection<Collection<DFSCode>> codeSiblings) {

    this.adjacencyLists = adjacencyLists;
    this.codeEmbeddings = codeEmbeddings;
    this.codeSiblings = codeSiblings;
  }

  public Map<Integer, AdjacencyList> getAdjacencyLists() {
    return adjacencyLists;
  }

  public Map<CompressedDFSCode, Collection<DFSEmbedding>>
  getCodeEmbeddings() {

    return codeEmbeddings;
  }

  public void setCodeEmbeddings(
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings) {

    this.codeEmbeddings = codeEmbeddings;
  }

  @Override
  public String toString() {

    StringBuilder builder = new StringBuilder("TransactionWrapper");


    builder.append(" (Graph)\n\tAdjacency lists");

    int vertexIndex = 0;
    for (AdjacencyList entry : getAdjacencyLists().values()) {

      builder.append("\n\t\t(" + entry.getVertexLabel() + ":" +
        vertexIndex + ") : " +
        StringUtils.join(entry.getEntries(), " | "));

      vertexIndex++;
    }

    builder.append("\n\tDFS codes and embeddings");

    for (Map.Entry<CompressedDFSCode, Collection<DFSEmbedding>> entry :
      getCodeEmbeddings().entrySet()) {

      builder.append("\n\t\t" + entry.getKey().getDfsCode());

      for (DFSEmbedding embedding : entry.getValue()) {
        builder.append(embedding);
      }
    }

    return builder.toString();
  }
}
