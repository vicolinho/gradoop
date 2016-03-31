package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.functions.IsActive;
import org.gradoop.model.impl.algorithms.fsm.functions.IsCollector;
import org.gradoop.model.impl.algorithms.fsm.functions.ConcatCompressedDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.DfsDecoder;
import org.gradoop.model.impl.algorithms.fsm.functions.EdgeExpander;
import org.gradoop.model.impl.algorithms.fsm.functions.ExpandFrequentDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphElements;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphSimpleEdge;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphSimpleVertex;
import org.gradoop.model.impl.algorithms.fsm.functions.GrowEmbeddings;
import org.gradoop.model.impl.algorithms.fsm.functions.MinCount;
import org.gradoop.model.impl.algorithms.fsm.functions.Report;
import org.gradoop.model.impl.algorithms.fsm.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.functions.SetCountToZero;
import org.gradoop.model.impl.algorithms.fsm.functions.VertexExpander;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleVertex;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;

public class GSpan
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  private final FSMConfig fsmConfig;
  private GradoopFlinkConfig<G, V, E> gradoopConfig;

  protected DataSet<Integer> minCount;

  public GSpan(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection)
  {
    this.gradoopConfig = collection.getConfig();
    setMinCount(collection);

    // pre processing
    DataSet<SearchSpaceItem> searchSpace = gradoopConfig
      .getExecutionEnvironment()
      .fromElements(SearchSpaceItem.createCollector())
      .union(expandGraphs(collection));

    // init iteration
    DeltaIteration<SearchSpaceItem, SearchSpaceItem> iteration = searchSpace
      .iterateDelta(searchSpace, 10, 0);

    DataSet<SearchSpaceItem> workset = iteration.getWorkset();

    // report DFS codes initially created or grown in last iteration
    DataSet<CompressedDfsCode[]> currentFrequentDfsCodes =
      workset
      .flatMap(new Report())      // report codes
      .groupBy(0)                 // group by code
      .sum(1)                     // count support
      .filter(new Frequent())     // filter by min support
      .withBroadcastSet(minCount, Frequent.DS_NAME)
      .map(new SetCountToZero())  // reuse tuple for grouping by ZERO
      .groupBy(1)                 // group by ZERO
      .reduceGroup(new ConcatCompressedDfsCodes());
                                  // concat frequent DFS codes

    // grow child embeddings of frequent DFS codes
    MapOperator<SearchSpaceItem, SearchSpaceItem> grownSearchSpace = workset
        .map(new GrowEmbeddings(fsmConfig))
        .withBroadcastSet(currentFrequentDfsCodes, GrowEmbeddings.DS_NAME);
        // broadcast frequent DFS codes to all graphs and the collector

    // filter graphs that grew embeddings
    DataSet<SearchSpaceItem> growableSearchSpace = grownSearchSpace
      .filter(new IsActive());

    // stop iterating
    // if no graph can grow child embeddings of frequent DFS codes
    DataSet<CompressedDfsCode> allFrequentDfsCodes = iteration
      .closeWith(grownSearchSpace, growableSearchSpace)
      .filter(new IsCollector())              // get only collector
      .flatMap(new ExpandFrequentDfsCodes()); // expand array to data set

    // post processing
    return createResultCollection(allFrequentDfsCodes);
  }

  protected void setMinCount(GraphCollection<G, V, E> collection) {
    this.minCount = Count
      .count(collection.getGraphHeads())
      .map(new MinCount(fsmConfig.getThreshold()));
  }

  private DataSet<SearchSpaceItem> expandGraphs(
    GraphCollection<G, V, E> collection) {

    DataSet<Tuple2<GradoopId, Collection<SimpleVertex>>> graphVertices =
      collection
        .getVertices()
        .flatMap(new GraphSimpleVertex<V>())
        .groupBy(0)
        .reduceGroup(new GraphElements<SimpleVertex>());

    DataSet<Tuple2<GradoopId, Collection<SimpleEdge>>> graphEdges =
      collection
        .getEdges()
        .flatMap(new GraphSimpleEdge<E>())
        .groupBy(0)
        .reduceGroup(new GraphElements<SimpleEdge>());

    return graphVertices
      .join(graphEdges)
      .where(0).equalTo(0)
      .with(new SearchSpace());
  }

  protected GraphCollection<G, V, E> createResultCollection(
    DataSet<CompressedDfsCode> allFrequentDfsCodes) {

    DataSet<Tuple3<G, Collection<V>, Collection<E>>> frequentSubgraphs =
      allFrequentDfsCodes
        .map(new DfsDecoder<>(
          gradoopConfig.getGraphHeadFactory(),
          gradoopConfig.getVertexFactory(),
          gradoopConfig.getEdgeFactory()
        ));

    DataSet<G> graphHeads = frequentSubgraphs
      .map(new Value0Of3<G, Collection<V>, Collection<E>>());

    DataSet<V> vertices = frequentSubgraphs
      .flatMap(new VertexExpander<G, V, E>())
      .returns(gradoopConfig.getVertexFactory().getType());

    DataSet<E> edges = frequentSubgraphs
      .flatMap(new EdgeExpander<G, V, E>())
      .returns(gradoopConfig.getEdgeFactory().getType());

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, gradoopConfig);
  }


  @Override
  public String getName() {
    return null;
  }
}
