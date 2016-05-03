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
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.functions.*;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions.PatternGrowth;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.Transaction;


import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class IterativeTransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TransactionalFSM<G, V, E> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public IterativeTransactionalFSM(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public GraphCollection<G, V, E>
  execute(GraphCollection<G, V, E> collection)  {
    setConfigAndMinSupport(collection);

    // pre processing
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges =
      pruneAndRelabelEdges(collection);

    // determine 1-edge frequent DFS patterns
    DataSet<CompressedDFSCode> allFrequentPatterns =
      find1EdgeFrequentDfsCodes(fatEdges);

    // filter edges by 1-edge DFS pattern
    fatEdges = filterFatEdges(fatEdges, allFrequentPatterns);

    // create search space with collector
    DataSet<Transaction> searchSpace = fatEdges
      .groupBy(0)
      .reduceGroup(new SearchSpace())
      .union(gradoopConfig.getExecutionEnvironment()
        .fromElements(Transaction.createCollector()));

    // ITERATION HEAD
    IterativeDataSet<Transaction> workSet = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY
    DataSet<Transaction> activeWorkSet = workSet
      .map(new PatternGrowth(fsmConfig))  // grow supported embeddings
      .filter(new IsActive());            // active, if at least one growth

    // determine frequent DFS patterns
    DataSet<Collection<CompressedDFSCode>> currentFrequentPatterns =
      activeWorkSet
        .flatMap(new ReportPatterns())  // report patterns
        .groupBy(0)                     // group by pattern
        .sum(1)                         // count support
        .filter(new Frequent())         // filter by min support
        .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
        .reduceGroup(new ConcatFrequentPatterns());

    // grow children of frequent DFS patterns
    DataSet<Transaction> nextWorkSet = activeWorkSet
      .map(new SupportPruning())    // drop embeddings of infrequent patterns
      .withBroadcastSet(
        currentFrequentPatterns, BroadcastNames.FREQUENT_PATTERNS)
      .filter(new IsActive());      // active, if at least one frequent pattern

    // ITERATION FOOTER
    DataSet<Transaction> collector = workSet
      // terminate, if no new frequent DFS patterns
      .closeWith(nextWorkSet, currentFrequentPatterns);

    // post processing
    allFrequentPatterns = collector
      .filter(new IsCollector())             // get only collector
      .flatMap(new ExpandFrequentDfsCodes()) // expand array to data set
      .union(allFrequentPatterns);

    return decodeDfsCodes(allFrequentPatterns);
  }

  @Override
  public String getName() {
    return "Transactional FSM using Bulk Iteration";
  }
}
