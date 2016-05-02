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
import org.gradoop.model.impl.algorithms.fsm.functions.*;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class IterativeGSpan
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends AbstractGSpan<G, V, E> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public IterativeGSpan(FSMConfig fsmConfig) {
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

    // create graph transactions from remaining edges
    DataSet<SearchSpaceItem> searchSpaceGraphs = fatEdges
      .groupBy(0)
      .reduceGroup(new IterativeSearchSpace());

    // create search space with collector
    DataSet<SearchSpaceItem> searchSpace = searchSpaceGraphs
      .union(gradoopConfig.getExecutionEnvironment()
        .fromElements(SearchSpaceItem.createCollector()));

    // ITERATION HEAD
    IterativeDataSet<SearchSpaceItem> workSet = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY
    DataSet<SearchSpaceItem> activeWorkSet = workSet
      .map(new PatternGrowth(fsmConfig))  // grow supported embeddings
      .filter(new IsActive());            // active, if at least one growth

    // determine frequent DFS codes
    DataSet<Collection<CompressedDFSCode>> iterationFrequentDfsCodes =
      activeWorkSet
        .flatMap(new ReportDfsCodes())  // report codes
        .groupBy(0)                     // group by code
        .sum(1)                         // count support
        .filter(new Frequent())         // filter by min support
        .withBroadcastSet(minSupport, Frequent.DS_NAME)
        .reduceGroup(new ConcatFrequentDfsCodes());

    // grow children of frequent DFS codes
    DataSet<SearchSpaceItem> nextWorkSet = activeWorkSet
      .map(new SupportPruning())    // drop embeddings of infrequent codes
      .withBroadcastSet(iterationFrequentDfsCodes, SupportPruning.DS_NAME)
      .filter(new IsActive());      // active, if at least one frequent code

    // ITERATION FOOTER
    DataSet<SearchSpaceItem> collector = workSet
      // terminate, if no new frequent DFS codes
      .closeWith(nextWorkSet, iterationFrequentDfsCodes);

    // post processing
    allFrequentDfsCodes = collector
      .filter(new IsCollector())             // get only collector
      .flatMap(new ExpandFrequentDfsCodes()) // expand array to data set
      .union(allFrequentDfsCodes);

    return decodeDfsCodes(allFrequentDfsCodes);
  }

  @Override
  public String getName() {
    return "Iterative gSpan implementation";
  }
}
