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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.functions.ConcatFrequentDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.ExpandFrequentDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphEdges;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphVertices;
import org.gradoop.model.impl.algorithms.fsm.functions.GrowEmbeddings;
import org.gradoop.model.impl.algorithms.fsm.functions.IsActive;
import org.gradoop.model.impl.algorithms.fsm.functions.IsCollector;
import org.gradoop.model.impl.algorithms.fsm.functions.ReportDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.IterativeSearchSpace;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
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

    setGradoopConfig(collection);
    setMinSupport(collection);

    // prepare search space
    DataSet<SearchSpaceItem> searchSpace = prepareSearchSpace(collection);

    // ITERATION HEAD
    IterativeDataSet<SearchSpaceItem> workSet = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY
    DataSet<SearchSpaceItem> activeWorkSet = workSet
      .filter(new IsActive());

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
    DataSet<SearchSpaceItem> nextWorkSet =
      activeWorkSet
        .map(new GrowEmbeddings(fsmConfig))
        .withBroadcastSet(iterationFrequentDfsCodes, GrowEmbeddings.DS_NAME);

    // ITERATION FOOTER
    DataSet<SearchSpaceItem> collector = workSet
      .closeWith(nextWorkSet, iterationFrequentDfsCodes);

    // post processing
    DataSet<CompressedDFSCode> allFrequentDfsCodes = collector
      .filter(new IsCollector())              // get only collector
      .flatMap(new ExpandFrequentDfsCodes()); // expand array to data set

    return decodeDfsCodes(allFrequentDfsCodes);
  }

  /**
   * dictionary creation, label encoding and vertex/edge pruning by label
   * frequency are triggered here; then, remaining vertices and edges are
   * combined to graph transactions; finally, the collector is added as a
   * special search space item
   *
   * @param collection input collection
   * @return search space
   */
  private DataSet<SearchSpaceItem> prepareSearchSpace(
    GraphCollection<G, V, E> collection) {
    // pre processing
    DataSet<Tuple2<GradoopId, ArrayList<Tuple2<GradoopId, Integer>>>>
      graphVertices = pruneAndRelabelVertices(collection)
      .groupBy(0)
      .reduceGroup(new GraphVertices());

    DataSet<Tuple2<GradoopId, ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>>
      graphEdges = pruneAndRelabelEdges(collection)
      .groupBy(0)
      .reduceGroup(new GraphEdges());

    // create graph transactions
    DataSet<SearchSpaceItem> searchSpaceGraphs =
      graphVertices
        .join(graphEdges)
        .where(0).equalTo(0)
        .with(new IterativeSearchSpace());

    // create collector
    DataSet<SearchSpaceItem> collector =
      gradoopConfig.getExecutionEnvironment()
        .fromElements(SearchSpaceItem.createCollector());

    return collector
      .union(searchSpaceGraphs);
  }


  @Override
  public String getName() {
    return "Iterative gSpan implementation";
  }
}
