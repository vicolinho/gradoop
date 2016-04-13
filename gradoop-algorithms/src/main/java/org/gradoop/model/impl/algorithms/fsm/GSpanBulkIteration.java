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
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.functions.ConcatFrequentDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.DfsDecoder;
import org.gradoop.model.impl.algorithms.fsm.functions.EdgeLabelGraphId;
import org.gradoop.model.impl.algorithms.fsm.functions.ExpandEdges;
import org.gradoop.model.impl.algorithms.fsm.functions.ExpandFrequentDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.ExpandVertices;
import org.gradoop.model.impl.algorithms.fsm.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.functions.FrequentLabel;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphElements;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphIdStringLabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphIdStringLabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.functions.GrowEmbeddings;
import org.gradoop.model.impl.algorithms.fsm.functions.IsActive;
import org.gradoop.model.impl.algorithms.fsm.functions.IsCollector;
import org.gradoop.model.impl.algorithms.fsm.functions.LabelTranslation;
import org.gradoop.model.impl.algorithms.fsm.functions.MinSupport;
import org.gradoop.model.impl.algorithms.fsm.functions.ReportDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.functions.VertexLabelGraphId;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.functions.tuple.Project0And2Of3;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class GSpanBulkIteration
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  /**
   * minimum support
   */
  protected DataSet<Integer> minSupport;
  /**
   * frequent subgraph mining configuration
   */
  private final FSMConfig fsmConfig;
  /**
   * Gradoop configuration
   */
  private GradoopFlinkConfig<G, V, E> gradoopConfig;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   * 
   */
  public GSpanBulkIteration(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public GraphCollection<G, V, E>
  execute(GraphCollection<G, V, E> collection)  {
    setGradoopConfig(collection);
    setMinSupport(collection);

    // pre processing
    DataSet<SearchSpaceItem<String>> searchSpaceGraphs =
      getSearchSpaceGraphs(collection);

    DataSource<SearchSpaceItem<String>> searchSpaceItemDataSource =
      gradoopConfig.getExecutionEnvironment()
        .fromElements(SearchSpaceItem.<String>createCollector());

    IterativeDataSet<SearchSpaceItem<String>> searchSpace =
      searchSpaceItemDataSource
      .union(searchSpaceGraphs)
      .iterate(fsmConfig.getMaxEdgeCount());

    DataSet<Collection<CompressedDFSCode<String>>> currentFrequentDfsCodes = 
      searchSpace
      .flatMap(new ReportDfsCodes<String>())  // report codes
      .groupBy(0)                     // group by code
      .sum(1)                         // count support
      .filter(new Frequent<String>())         // filter by min support
      .withBroadcastSet(minSupport, Frequent.DS_NAME)
      .reduceGroup(new ConcatFrequentDfsCodes<String>());

    DataSet<SearchSpaceItem<String>> growableSearchSpace = searchSpace
      .filter(new IsActive<String>())
      .map(new GrowEmbeddings<String>(fsmConfig))
      .withBroadcastSet(currentFrequentDfsCodes, GrowEmbeddings.DS_NAME);

    DataSet<SearchSpaceItem<String>> collector = searchSpace
      .closeWith(growableSearchSpace, currentFrequentDfsCodes);

    DataSet<CompressedDFSCode<String>> allFrequentDfsCodes = collector
      .filter(new IsCollector<String>())              // get only collector
      .flatMap(new ExpandFrequentDfsCodes<String>()); // expand array to data set

    // post processing
    return decodeDfsCodes(allFrequentDfsCodes);
  }

  private void setGradoopConfig(GraphCollection<G, V, E> collection) {
    this.gradoopConfig = collection.getConfig();
  }

  protected void setMinSupport(GraphCollection<G, V, E> collection) {
    this.minSupport = Count
      .count(collection.getGraphHeads())
      .map(new MinSupport(fsmConfig.getThreshold()));
  }

  /**
   * turns a graph collection into a data set of search space items
   * @param collection input collection
   * @return search space
   */
  private DataSet<SearchSpaceItem<String>> getSearchSpaceGraphs(
    GraphCollection<G, V, E> collection) {

    // vertices

    DataSet<Tuple2<GradoopId, LabeledVertex<String>>> graphIdVertex = collection
      .getVertices()
      .flatMap(new GraphIdStringLabeledVertex<V>());

    DataSet<Tuple2<String, Integer>> vertexLabelDictionary =
      graphIdVertex
        .map(new VertexLabelGraphId())
        .distinct()
        .map(new Project0And2Of3<String, GradoopId, Integer>())
        .groupBy(0)
        .sum(1)
        .filter(new FrequentLabel())
        .withBroadcastSet(minSupport, Frequent.DS_NAME)
        .reduceGroup(new LabelTranslation());

    DataSet<Tuple2<GradoopId, Collection<LabeledVertex<String>>>>
      graphVertices =
      graphIdVertex
        .groupBy(0)
        .reduceGroup(new GraphElements<LabeledVertex<String>>());

    // edges

    DataSet<Tuple2<GradoopId, LabeledEdge<String>>> graphIdEdge = collection
      .getEdges()
      .flatMap(new GraphIdStringLabeledEdge<E>());

    DataSet<Tuple2<String, Integer>> edgeLabelDictionary =
      graphIdEdge
        .map(new EdgeLabelGraphId())
        .distinct()
        .map(new Project0And2Of3<String, GradoopId, Integer>())
        .groupBy(0)
        .sum(1)
        .filter(new FrequentLabel())
        .withBroadcastSet(minSupport, Frequent.DS_NAME)
        .reduceGroup(new LabelTranslation());

    DataSet<Tuple2<GradoopId, Collection<LabeledEdge<String>>>> graphEdges =
      graphIdEdge
        .groupBy(0)
        .reduceGroup(new GraphElements<LabeledEdge<String>>());

    return graphVertices
      .join(graphEdges)
      .where(0).equalTo(0)
      .with(new SearchSpace<String>());
  }

  /**
   * turns a data set of DFS codes into a graph collection
   * @param dfsCodes DFS code data set
   * @return graph collection
   */
  protected GraphCollection<G, V, E> decodeDfsCodes(
    DataSet<CompressedDFSCode<String>> dfsCodes) {

    DataSet<Tuple3<G, Collection<V>, Collection<E>>> frequentSubgraphs =
      dfsCodes
        .map(new DfsDecoder<G, V ,E , String>(
          gradoopConfig.getGraphHeadFactory(),
          gradoopConfig.getVertexFactory(),
          gradoopConfig.getEdgeFactory()
        ));

    DataSet<G> graphHeads = frequentSubgraphs
      .map(new Value0Of3<G, Collection<V>, Collection<E>>());

    DataSet<V> vertices = frequentSubgraphs
      .flatMap(new ExpandVertices<G, V, E>())
      .returns(gradoopConfig.getVertexFactory().getType());

    DataSet<E> edges = frequentSubgraphs
      .flatMap(new ExpandEdges<G, V, E>())
      .returns(gradoopConfig.getEdgeFactory().getType());

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, gradoopConfig);
  }


  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
