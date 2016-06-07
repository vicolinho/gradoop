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


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .SerializedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;

/**
 * G => compress(G) IF G is canonical
 */
public class PostPruneAndCompress implements FlatMapFunction
  <WithCount<SerializedSubgraph>, WithCount<CompressedSubgraph>> {

  /**
   * frequent subgraph mining configuration
   */
  private final FsmConfig fsmConfig;

  /**
   * constructor
   *
   * @param fsmConfig frequent subgraph mining configuration
   */
  public PostPruneAndCompress(FsmConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(WithCount<SerializedSubgraph> subgraph,
    Collector<WithCount<CompressedSubgraph>> collector) throws Exception {

    DfsCode code = subgraph.getObject().getDfsCode();
    int support = subgraph.getCount();

    if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
      collector.collect(new WithCount<>(new CompressedSubgraph(code), support));
    }
  }
}
