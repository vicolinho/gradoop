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

package org.gradoop.model.impl.algorithms.fsm.iterative.tuples;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
public class TransactionWrapper extends
  Tuple4<Boolean, Boolean, Transaction, List<CompressedDFSCode>> {

  /**
   * default constructor
   */
  public TransactionWrapper() {
  }

  /**
   * valued constructor
   *
   * @param collector        true for collector, false for graph
   * @param active           true for active, false for inactive
   * @param frequentDfsCodes frequent DFS codes (empty for graph)
   */
  public TransactionWrapper(boolean collector, boolean active, Transaction transaction,
    List<CompressedDFSCode> frequentDfsCodes) {

    setCollector(collector);
    setActive(active);
    setTransaction(transaction);
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
      getTransaction().getCodeEmbeddings().clear();
      getTransaction().getAdjacencyLists().clear();
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

  public Transaction getTransaction() {
    return this.f2;
  }

  public void setTransaction(Transaction transaction) {
    this.f2 = transaction;
  }

  public List<CompressedDFSCode> getFrequentDfsCodes() {
    return this.f3;
  }

  public void setFrequentDfsCodes(
    List<CompressedDFSCode> collectedDfsCodes) {
    this.f3 = collectedDfsCodes;
  }

  /**
   * factory method
   * @param adjacencyLists adjacency lists
   * @param codeEmbeddings embeddings of DFS codes
   * @param codeSiblings
   * @return a search space item representing a graph transaction
   */
  public static TransactionWrapper createForGraph(
    Map<Integer, AdjacencyList> adjacencyLists,
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings,
    Collection<Collection<DFSCode>> codeSiblings) {

    return new TransactionWrapper(false, true,
      new Transaction(adjacencyLists, codeEmbeddings, codeSiblings),
      new ArrayList<CompressedDFSCode>());
  }
  public static TransactionWrapper createForGraph(Transaction transaction) {
    ArrayList<CompressedDFSCode> emptyList =
      Lists.newArrayListWithExpectedSize(0);

    return new TransactionWrapper(false, true, transaction, emptyList);
  }

  /**
   * factory method
   * @return a search space item representing the collector
   */
  public static TransactionWrapper createCollector() {
    Map<Integer, AdjacencyList> adjacencyLists =
      Maps.newHashMapWithExpectedSize(0);

    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMapWithExpectedSize(0);

    Collection<Collection<DFSCode>> codeSiblings =
      Lists.newArrayListWithExpectedSize(0);

    List<CompressedDFSCode> frequentDfsCodes = Lists.newArrayList();

    return new TransactionWrapper(true, true,
      new Transaction(adjacencyLists, codeEmbeddings, codeSiblings),
      frequentDfsCodes);
  }


}
