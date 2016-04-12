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
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledVertex;
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
    IterativeDataSet<SearchSpaceItem> searchSpace = gradoopConfig
      .getExecutionEnvironment()
      .fromElements(SearchSpaceItem.createCollector())
      .union(getSearchSpaceGraphs(collection))
      .iterate(fsmConfig.getMaxEdgeCount());

    // report DFS codes initially created or grown in last iteration
    DataSet<CompressedDFSCode[]> currentFrequentDfsCodes = searchSpace
      .flatMap(new ReportDfsCodes())  // report codes
      .groupBy(0)                     // group by code
      .sum(1)                         // count support
      .filter(new Frequent())         // filter by min support
      .withBroadcastSet(minSupport, Frequent.DS_NAME)
      .reduceGroup(new ConcatFrequentDfsCodes());
    // concat frequent DFS codes

    // grow child embeddings of frequent DFS codes
    DataSet<SearchSpaceItem> growableSearchSpace = searchSpace
      .filter(new IsActive())
//      .cross(currentFrequentDfsCodes)
//      .with(new GrowEmbeddings(fsmConfig));
      .map(new GrowEmbeddings(fsmConfig))
      .withBroadcastSet(currentFrequentDfsCodes, GrowEmbeddings.DS_NAME);

    DataSet<SearchSpaceItem> collector = searchSpace
      .closeWith(growableSearchSpace, currentFrequentDfsCodes);

    DataSet<CompressedDFSCode> allFrequentDfsCodes = collector
      .filter(new IsCollector())              // get only collector
      .flatMap(new ExpandFrequentDfsCodes()); // expand array to data set

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
  private DataSet<SearchSpaceItem> getSearchSpaceGraphs(
    GraphCollection<G, V, E> collection) {

    // vertices

    DataSet<Tuple2<GradoopId, StringLabeledVertex>> graphIdVertex = collection
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

    DataSet<Tuple2<GradoopId, Collection<StringLabeledVertex>>> graphVertices =
      graphIdVertex
        .groupBy(0)
        .reduceGroup(new GraphElements<StringLabeledVertex>());

    // edges

    DataSet<Tuple2<GradoopId, StringLabeledEdge>> graphIdEdge = collection
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

    DataSet<Tuple2<GradoopId, Collection<StringLabeledEdge>>> graphEdges =
      graphIdEdge
        .groupBy(0)
        .reduceGroup(new GraphElements<StringLabeledEdge>());

    return graphVertices
      .join(graphEdges)
      .where(0).equalTo(0)
      .with(new SearchSpace());
  }

  /**
   * turns a data set of DFS codes into a graph collection
   * @param dfsCodes DFS code data set
   * @return graph collection
   */
  protected GraphCollection<G, V, E> decodeDfsCodes(
    DataSet<CompressedDFSCode> dfsCodes) {

    DataSet<Tuple3<G, Collection<V>, Collection<E>>> frequentSubgraphs =
      dfsCodes
        .map(new DfsDecoder<>(
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
