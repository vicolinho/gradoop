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
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.functions.AppendSourceLabel;
import org.gradoop.model.impl.algorithms.fsm.functions.AppendTargetLabel;
import org.gradoop.model.impl.algorithms.fsm.functions.BuildAdjacencyLists;
import org.gradoop.model.impl.algorithms.fsm.functions.CountableTriplePattern;
import org.gradoop.model.impl.algorithms.fsm.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.functions.FrequentTriplePattern;
import org.gradoop.model.impl.algorithms.fsm.functions.LocalDfsCodeSupports;
import org.gradoop.model.impl.algorithms.fsm.functions.LocalFSM;
import org.gradoop.model.impl.algorithms.fsm.functions.SearchSpacePartitioner;
import org.gradoop.model.impl.algorithms.fsm.functions.AddTaskId;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpacePartition;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.tuple.Project3To1And2;
import org.gradoop.model.impl.functions.tuple.Project4To0And1And2;
import org.gradoop.model.impl.functions.tuple.Project6To0And3And4And5;
import org.gradoop.model.impl.id.GradoopId;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class FilterAndRefineGSpan
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends AbstractGSpan<G, V, E> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public FilterAndRefineGSpan(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public GraphCollection<G, V, E>
  execute(GraphCollection<G, V, E> collection)  {
    setGradoopConfig(collection);
    setMinSupport(collection);

    // prepare search space
    DataSet<AdjacencyLists> prunedAdjacencyLists =
      getPrunedAdjacencyLists(collection);

    DataSet<SearchSpacePartition> searchSpace = prunedAdjacencyLists
      .map(new AddTaskId())
      .groupBy(0) // task id
      .combineGroup(new SearchSpacePartitioner())
      .map(new LocalFSM())
      .withBroadcastSet(minSupport, LocalFSM.DS_NAME);

    // refinement

    DataSet<CompressedDFSCode> allFrequentDfsCodes = searchSpace
      .flatMap(new LocalDfsCodeSupports())
      .groupBy(0)                     // group by code
      .sum(1)                         // count support
      .filter(new Frequent())         // filter by min support
      .withBroadcastSet(minSupport, Frequent.DS_NAME);

    return decodeDfsCodes(allFrequentDfsCodes);
  }

  /**
   * @param collection input collection
   * @return search space
   */
  private DataSet<AdjacencyLists> getPrunedAdjacencyLists(
    GraphCollection<G, V, E> collection) {

    // pre processing
    DataSet<Tuple2<GradoopId, Integer>> vertices =
      pruneAndRelabelVertices(collection)
      .map(new Project3To1And2<GradoopId, GradoopId, Integer>())
      .distinct();

    DataSet<Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>>
      edges = pruneAndRelabelEdges(collection)
      .join(vertices)
      .where(1).equalTo(0)
      .with(new AppendSourceLabel())
      .join(vertices)
      .where(2).equalTo(0)
      .with(new AppendTargetLabel());

    DataSet<Tuple3<Integer, Integer, Integer>>
      frequentTriplePatterns = edges
      .map(new Project6To0And3And4And5
        <GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>())
      .distinct()
      .map(new CountableTriplePattern())
      .groupBy(0, 1, 2)
      .sum(3)
      .filter(new FrequentTriplePattern())
      .withBroadcastSet(minSupport, FrequentTriplePattern.DS_NAME)
      .map(new Project4To0And1And2<Integer, Integer, Integer, Integer>());

    edges = edges
      .join(frequentTriplePatterns)
      .where(3, 4, 5).equalTo(0, 1, 2)
      .with(new LeftSide<
        Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>,
        Tuple3<Integer, Integer, Integer>>());

    DataSet<AdjacencyLists> adjacencyLists = edges
      .groupBy(0)
      .reduceGroup(new BuildAdjacencyLists());

    return adjacencyLists;
  }


  @Override
  public String getName() {
    return "Iterative gSpan implementation";
  }
}
