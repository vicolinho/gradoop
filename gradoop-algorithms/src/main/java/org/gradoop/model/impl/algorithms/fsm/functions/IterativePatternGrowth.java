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

package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.IterativeSearchSpaceItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Core of gSpan implementation. Grows embeddings of Frequent DFS codes.
 */
public class IterativePatternGrowth
  implements MapFunction<IterativeSearchSpaceItem, IterativeSearchSpaceItem> {

  private final PatternGrower grower;

  /**
   * constructor
   * @param fsmConfig configuration
   */
  public IterativePatternGrowth(FSMConfig fsmConfig) {
    this.grower = new PatternGrower(fsmConfig);
  }

  @Override
  public IterativeSearchSpaceItem map(IterativeSearchSpaceItem searchSpaceItem)
    throws Exception {

    if (! searchSpaceItem.isCollector()) {
      growFrequentDfsCodeEmbeddings(searchSpaceItem);
    }

    return searchSpaceItem;
  }

  /**
   * grows all embeddings of frequent DFS codes in a graph
   * @param graph graph search space item
   * @return graph with grown embeddings
   */
  private IterativeSearchSpaceItem growFrequentDfsCodeEmbeddings(
    IterativeSearchSpaceItem graph) {

    ArrayList<AdjacencyList> adjacencyLists = graph.getAdjacencyLists();

    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> parentEmbeddings =
      graph.getCodeEmbeddings();

    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> childEmbeddings =
      grower.growEmbeddings(adjacencyLists, parentEmbeddings);

    graph.setCodeEmbeddings(childEmbeddings);
    graph.setActive(! childEmbeddings.isEmpty());

    return graph;
  }
}
