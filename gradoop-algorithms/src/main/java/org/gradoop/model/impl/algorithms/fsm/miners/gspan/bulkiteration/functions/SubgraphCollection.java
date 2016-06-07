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

package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration
  .pojos.IterationItem;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * GROUP REDUCE:
 * (g1, freq1),..,(gN, freqN) => [(g1, freq1),..,(gN, freqN)]
 *
 * MAP:
 * IterationItem[(g1, freq1),..,(gN, freqN)] => (g1, freq1),..,(gN, freqN)
 *
 * REDUCE:
 * [(g1, freq1),..,(gN, freqN)],..,[(g1, freqN+1),..,(gM, freqM)]
 * => [(g1, freq1),..,(gM, freqM)]
 */
public class SubgraphCollection implements GroupReduceFunction
  <WithCount<CompressedSubgraph>, Collection<WithCount<CompressedSubgraph>>>,
  MapFunction<IterationItem, Collection<WithCount<CompressedSubgraph>>>,
  ReduceFunction<Collection<WithCount<CompressedSubgraph>>> {

  /**
   * Frequent Subgraph Mining configuration
   */
  private FsmConfig fsmConfig;

  /**
   * default constructor
   */
  public SubgraphCollection() {
  }

  /**
   * valued constructor
   *
   * @param fsmConfig Frequent Subgraph Mining configuration
   */
  public SubgraphCollection(FsmConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public Collection<WithCount<CompressedSubgraph>> map(
    IterationItem collector) throws Exception {

    return collector.getFrequentSubgraphs();
  }

  @Override
  public void reduce(Iterable<WithCount<CompressedSubgraph>> iterable,
    Collector<Collection<WithCount<CompressedSubgraph>>> collector) throws
    Exception {

    List<WithCount<CompressedSubgraph>> subgraphs = Lists.newArrayList();
    Iterator<WithCount<CompressedSubgraph>> iterator = iterable.iterator();

    if (iterator.hasNext()) {
      WithCount<CompressedSubgraph> subgraph = iterator.next();

      if (subgraph.getObject().getDfsCode().size() >=
        fsmConfig.getMinEdgeCount()) {
        subgraphs.add(subgraph);

        while (iterator.hasNext()) {
          subgraphs.add(iterator.next());
        }
      }
    }

    collector.collect(subgraphs);
  }

  @Override
  public Collection<WithCount<CompressedSubgraph>> reduce(
    Collection<WithCount<CompressedSubgraph>> firstSubgraphs,
    Collection<WithCount<CompressedSubgraph>> secondSubgraphs) throws
    Exception {

    Collection<WithCount<CompressedSubgraph>> mergedSubgraphs;

    if (firstSubgraphs.size() >= firstSubgraphs.size()) {
      firstSubgraphs.addAll(secondSubgraphs);
      mergedSubgraphs = firstSubgraphs;
    } else {
      secondSubgraphs.addAll(firstSubgraphs);
      mergedSubgraphs = secondSubgraphs;
    }

    return mergedSubgraphs;
  }
}
