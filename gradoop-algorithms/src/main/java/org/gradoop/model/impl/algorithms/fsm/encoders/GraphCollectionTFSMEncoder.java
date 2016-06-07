package org.gradoop.model.impl.algorithms.fsm.encoders;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.functions.Frequent;
import org.gradoop.model.impl.functions.utils.AddCount;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.FlipDictionary;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.MinFrequency;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.AppendSourceLabel;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.AppendTargetLabel;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.CountableTriple;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.Dictionary;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.EdgeLabelEncoder;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.FrequentLabelTriple;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.GraphIdElementIdLabel;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.VertexLabelEncoder;
import org.gradoop.model.impl.algorithms.fsm.encoders.functions.WithoutVertexIds;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples
  .EdgeTripleWithoutVertexLabels;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.LabelTripleWithCount;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.VertexIdLabel;
import org.gradoop.model.impl.functions.tuple.Value1Of2;
import org.gradoop.model.impl.functions.utils.RightSide;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;

import java.util.List;
import java.util.Map;

public class GraphCollectionTFSMEncoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements TransactionalFSMEncoder<GraphCollection<G, V, E>> {

  /**
   * minimum support
   */
  private DataSet<Integer> minFrequency;

  /**
   * edge label dictionary
   */
  private DataSet<List<String>> edgeLabelDictionary;
  /**
   * vertex label dictionary
   */
  private DataSet<List<String>> vertexLabelDictionary;

  /**
   * determines edge label frequency and prunes by minimum frequency;
   * label frequencies are used to relabel edges where higher frequency leads
   * to a smaller numeric label;
   *
   * @return pruned and relabelled edges
   */
  @Override
  public DataSet<EdgeTriple> encode(
    GraphCollection<G, V, E> collection, FSMConfig fsmConfig) {

    setMinFrequency(collection, fsmConfig);

    DataSet<VertexIdLabel> encodedVertices =
      encodeVertices(collection.getVertices());

    DataSet<EdgeTripleWithoutVertexLabels> encodedEdges =
      encodeEdges(collection.getEdges());

    DataSet<EdgeTriple> triples = combine(encodedVertices, encodedEdges);

    return frequent(triples);
  }

  private void setMinFrequency(GraphCollection<G, V, E> collection,
    FSMConfig fsmConfig) {
    this.minFrequency = Count
      .count(collection.getGraphHeads())
      .map(new MinFrequency(fsmConfig));
  }

  private DataSet<EdgeTripleWithoutVertexLabels> encodeEdges(
    DataSet<E> edges) {

    edgeLabelDictionary = edges
      .flatMap(new GraphIdElementIdLabel<E>())
      .distinct()
      .map(new Value1Of2<GradoopId, String>())
      .map(new AddCount<String>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<String>())
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .reduceGroup(new Dictionary());

    DataSet<Map<String, Integer>> reverseDictionary = edgeLabelDictionary
      .map(new FlipDictionary());

    return edges
      .flatMap(new EdgeLabelEncoder<E>())
      .withBroadcastSet(reverseDictionary, BroadcastNames.EDGE_DICTIONARY);
  }

  /**
   * determines vertex label frequency and prunes by minimum frequency;
   * label frequencies are used to relabel vertices where higher frequency leads
   * to a smaller numeric label;
   *
   * @param vertices input vertex collection
   * @return pruned and relabelled edges
   */
  private DataSet<VertexIdLabel> encodeVertices(DataSet<V> vertices) {

    vertexLabelDictionary = vertices
      .flatMap(new GraphIdElementIdLabel<V>())
      .distinct()
      .map(new Value1Of2<GradoopId, String>())
      .map(new AddCount<String>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<String>())
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .reduceGroup(new Dictionary());

    DataSet<Map<String, Integer>> reverseDictionary = vertexLabelDictionary
      .map(new FlipDictionary());

    return vertices
      .flatMap(new VertexLabelEncoder<V>())
      .withBroadcastSet(reverseDictionary, BroadcastNames.VERTEX_DICTIONARY);
  }

  private DataSet<EdgeTriple> combine(DataSet<VertexIdLabel> encodedVertices,
    DataSet<EdgeTripleWithoutVertexLabels> encodedEdges) {

    return encodedEdges
      .join(encodedVertices).where(1).equalTo(0)
      .with(new AppendSourceLabel())
      .join(encodedVertices).where(2).equalTo(0)
      .with(new AppendTargetLabel());
  }

  private DataSet<EdgeTriple> frequent(DataSet<EdgeTriple> triples) {
    return triples
      .map(new WithoutVertexIds())
      .distinct()
      .map(new CountableTriple())
      .groupBy(0, 1, 2)
      .sum(3)
      .filter(new FrequentLabelTriple())
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .join(triples)
      .where(0, 1, 2).equalTo(3, 4, 5)
      .with(new RightSide<LabelTripleWithCount, EdgeTriple>());
  }

  public DataSet<Integer> getMinFrequency() {
    return minFrequency;
  }

  @Override
  public DataSet<List<String>> getVertexLabelDictionary() {
    return vertexLabelDictionary;
  }

  @Override
  public DataSet<List<String>> getEdgeLabelDictionary() {
    return edgeLabelDictionary;
  }
}
