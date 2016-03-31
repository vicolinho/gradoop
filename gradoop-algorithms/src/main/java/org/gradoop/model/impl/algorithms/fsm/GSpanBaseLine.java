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
import org.gradoop.model.impl.algorithms.fsm.functions.Active;
import org.gradoop.model.impl.algorithms.fsm.functions.CollectorItem;
import org.gradoop.model.impl.algorithms.fsm.functions.ConcatCompressedDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.functions.DfsDecoder;
import org.gradoop.model.impl.algorithms.fsm.functions.EdgeExpander;
import org.gradoop.model.impl.algorithms.fsm.functions.ExpandCollector;
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

public class GSpanBaseLine
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  private final float threshold;
  protected DataSet<Integer> minCount;
  private GradoopFlinkConfig<G, V, E> config;

  public GSpanBaseLine(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection)
  {

    this.config = collection.getConfig();
    setMinCount(collection);

    DataSet<SearchSpaceItem> searchSpace = getSearchSpace(collection);

    DeltaIteration<SearchSpaceItem, SearchSpaceItem> iteration = searchSpace
      .iterateDelta(searchSpace, 3, 0);

    DeltaIteration.WorksetPlaceHolder<SearchSpaceItem> workset =
      iteration.getWorkset();

    DataSet<CompressedDfsCode[]> currentFrequentDfsCodes =
      workset
      .flatMap(new Report())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent())
      .withBroadcastSet(minCount, Frequent.DS_NAME)
      .map(new SetCountToZero())
      .groupBy(1)
      .reduceGroup(new ConcatCompressedDfsCodes());

    MapOperator<SearchSpaceItem, SearchSpaceItem> grownSearchSpace = workset
        .map(new GrowEmbeddings())
        .withBroadcastSet(currentFrequentDfsCodes, GrowEmbeddings.DS_NAME);

    DataSet<SearchSpaceItem> growableSearchSpace = grownSearchSpace
      .filter(new Active());

    DataSet<CompressedDfsCode> allFrequentDfsCodes = iteration
      .closeWith(grownSearchSpace, growableSearchSpace)
      .filter(new CollectorItem())
      .flatMap(new ExpandCollector());

    DataSet<Tuple3<G, Collection<V>, Collection<E>>> frequentSubgraphs =
      allFrequentDfsCodes
        .map(new DfsDecoder<>(
          config.getGraphHeadFactory(),
          config.getVertexFactory(),
          config.getEdgeFactory()
        ));

    return createResultCollection(frequentSubgraphs);
  }

  protected void setMinCount(GraphCollection<G, V, E> collection) {
    this.minCount = Count
      .count(collection.getGraphHeads())
      .map(new MinCount(threshold));
  }

  private DataSet<SearchSpaceItem> getSearchSpace(
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
    DataSet<Tuple3<G, Collection<V>, Collection<E>>> frequentSubgraphs) {

    DataSet<G> graphHeads = frequentSubgraphs
      .map(new Value0Of3<G, Collection<V>, Collection<E>>());

    DataSet<V> vertices = frequentSubgraphs
      .flatMap(new VertexExpander<G, V, E>())
      .returns(config.getVertexFactory().getType());

    DataSet<E> edges = frequentSubgraphs
      .flatMap(new EdgeExpander<G, V, E>())
      .returns(config.getEdgeFactory().getType());

    return GraphCollection.fromDataSets(graphHeads, vertices, edges, config);
  }


  @Override
  public String getName() {
    return null;
  }
}
