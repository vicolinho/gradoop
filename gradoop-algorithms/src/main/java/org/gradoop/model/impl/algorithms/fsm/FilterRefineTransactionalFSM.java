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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.functions.*;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;
import org.gradoop.model.impl.functions.bool.False;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Map;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class FilterRefineTransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TransactionalFSM<G, V, E> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public FilterRefineTransactionalFSM(FSMConfig fsmConfig) {
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

    // distribute graphs to workers
    DataSet<Tuple2<Integer, Map<Integer, Transaction>>> partitions = fatEdges
      // group by graphId and create transaction for each graph
      .groupBy(0)
      .reduceGroup(new SearchSpace())
      // partition transactions
      .mapPartition(new SearchSpacePartition());

    // get worker ids with local graph counts
    DataSet<Map<Integer, Integer>> workerIdsGraphCount = partitions
      .reduceGroup(new WorkerIdGraphCount());

    // FILTER round
    DataSet<Tuple3<CompressedDFSCode, Integer, Boolean>> candidates =
      partitions
        // run local FSM
        .flatMap(new LocalTransactionalFSM(fsmConfig))
        // group reports by DFS code
        .groupBy("0.0")
        // keep if sure or likely globally frequent; drop otherwise
        .reduceGroup(new FrequentOrRefinementCandidate(fsmConfig))
        .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
        .withBroadcastSet(
          workerIdsGraphCount, BroadcastNames.WORKER_GRAPHCOUNT);

    // add globally frequent DFS codes to result
    DataSet<CompressedDFSCode> newFrequent = candidates
      .filter(new KnownToBeGloballyFrequent())
      .map(new Value0Of3<CompressedDFSCode, Integer, Boolean>());

    allFrequentDfsCodes = newFrequent
      .union(allFrequentDfsCodes);

    // REFINEMENT

    DataSet<Tuple3<CompressedDFSCode, Integer, Boolean>> refinementCandidates =
      candidates
        .filter(new NeedsRefinement());

    DataSet<CompressedDFSCode> refinementCalls =
      refinementCandidates
        .filter(new RefinementCall())
      .groupBy(1)
      .reduceGroup(new RefinementCalls())
      .join(partitions)
      .where(0).equalTo(0)
      .with(new Refinement());

    DataSet<CompressedDFSCode> incompleteResults = refinementCandidates
      .filter(new IncompleteResult())
      .map(new Value0Of3<CompressedDFSCode, Integer, Boolean>());

    // DEBUG
    allFrequentDfsCodes = allFrequentDfsCodes
      .union(refinementCalls.filter(new False<CompressedDFSCode>()));

    return decodeDfsCodes(allFrequentDfsCodes);
  }

  @Override
  public String getName() {
    return "Naive parallel Transactional FSM";
  }
}
