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

package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.naiveparallel.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.common.functions.LocalTransactionalFSM;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.naiveparallel.pojos.Transaction;
import org.gradoop.model.impl.id.GradoopId;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class NaiveParallelTransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TransactionalFSM<G, V, E> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public NaiveParallelTransactionalFSM(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public GraphCollection<G, V, E>
  execute(GraphCollection<G, V, E> collection)  {
    setConfigAndMinSupport(collection);

    // pre processing
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges =
      pruneAndRelabelEdges(collection);

    // determine 1-edge frequent DFS codes
    DataSet<CompressedDFSCode> allFrequentDfsCodes =
      find1EdgeFrequentDfsCodes(fatEdges);

    // filter edges by 1-edge DFS code
    fatEdges = filterFatEdges(fatEdges, allFrequentDfsCodes);

    // create search space
    DataSet<Transaction> searchSpace = fatEdges
      .groupBy(0)
      .reduceGroup(new SearchSpace());

    allFrequentDfsCodes = searchSpace
      // distribute graphs among workers and run local FSM
      .combineGroup(new LocalTransactionalFSM(fsmConfig))
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      .groupBy(0)                     // group by code
      .sum(1)                         // count support
      .filter(new Frequent())         // filter by min support
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      .union(allFrequentDfsCodes);

    return decodeDfsCodes(allFrequentDfsCodes);
  }

  @Override
  public String getName() {
    return "Naive parallel Transactional FSM";
  }
}
