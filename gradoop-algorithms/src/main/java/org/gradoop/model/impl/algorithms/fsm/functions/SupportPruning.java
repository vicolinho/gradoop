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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.IterativeSearchSpaceItem;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Core of gSpan implementation. Grows embeddings of Frequent DFS codes.
 */
public class SupportPruning extends
  RichMapFunction<IterativeSearchSpaceItem, IterativeSearchSpaceItem> {

  /**
   * broadcasting dataset name
   */
  public static final String DS_NAME = "compressedDfsCodes";
  /**
   * frequent DFS codes
   */
  private Collection<CompressedDFSCode> frequentDfsCodes;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Collection<CompressedDFSCode>> broadcast = getRuntimeContext()
      .getBroadcastVariable(DS_NAME);

    if (broadcast.isEmpty()) {
      this.frequentDfsCodes = null;
    } else {
      this.frequentDfsCodes = broadcast.get(0);
    }
  }

  @Override
  public IterativeSearchSpaceItem map(IterativeSearchSpaceItem searchSpaceItem) throws Exception {

    if (frequentDfsCodes != null) {
      if (searchSpaceItem.isCollector()) {
        searchSpaceItem = updateCollector(searchSpaceItem, frequentDfsCodes);
      } else {
        searchSpaceItem = dropInfrequentEmbeddings(
          searchSpaceItem, frequentDfsCodes);
      }
    }
    return searchSpaceItem;
  }

  /**
   * appends frequent DFS codes collected so far by new ones
   * @param collector collector search space item
   * @param newFrequentDfsCodes new frequent DFS codes
   * @return updated collector
   */
  private IterativeSearchSpaceItem updateCollector(IterativeSearchSpaceItem collector,
    Collection<CompressedDFSCode> newFrequentDfsCodes) {

    collector.getFrequentDfsCodes().addAll(newFrequentDfsCodes);

    return collector;
  }

  /**
   * grows all embeddings of frequent DFS codes in a graph
   * @param graph graph search space item
   * @param frequentDfsCodes frequent DFS codes
   * @return graph with grown embeddings
   */
  private IterativeSearchSpaceItem dropInfrequentEmbeddings(IterativeSearchSpaceItem graph,
    Collection<CompressedDFSCode> frequentDfsCodes) {

    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings =
      graph.getCodeEmbeddings();

    Collection<CompressedDFSCode> supportedCodes = Lists
      .newArrayList(codeEmbeddings.keySet());

    for (CompressedDFSCode supportedCode : supportedCodes) {
      if (! frequentDfsCodes.contains(supportedCode)) {
        codeEmbeddings.remove(supportedCode);
      }
    }

    graph.setActive(! codeEmbeddings.isEmpty());

    return graph;
  }
}
