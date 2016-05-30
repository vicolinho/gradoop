package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.common.functions.CountableLabel;
import org.gradoop.model.impl.algorithms.fsm.common.functions.FlipDictionary;
import org.gradoop.model.impl.algorithms.fsm.common.functions.FrequentLabel;
import org.gradoop.model.impl.algorithms.fsm.common.functions.MinSupport;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.AppendSourceLabel;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.AppendTargetLabel;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.CountableTriple;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.Dictionary;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.EdgeLabelEncoder;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.FrequentLabelTriple;
import org.gradoop.model.impl.algorithms.fsm.pre.functions
  .GraphIdElementIdLabel;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.VertexLabelEncoder;
import org.gradoop.model.impl.algorithms.fsm.pre.functions.WithoutVertexIds;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples
  .EdgeTripleWithoutVertexLabels;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.LabelTripleWithSupport;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.VertexIdLabel;
import org.gradoop.model.impl.functions.utils.RightSide;
import org.gradoop.model.impl.operators.count.Count;

import java.util.List;
import java.util.Map;

public class GradoopTransactionalFSMEncoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements TransactionalFSMEncoder<GraphCollection<G, V, E>> {

  /**
   * minimum support
   */
  protected DataSet<Integer> minSupport;

  /**
   * edge label dictionary
   */
  private DataSet<List<String>> edgeLabelDictionary;
  /**
   * vertex label dictionary
   */
  private DataSet<List<String>> vertexLabelDictionary;

  /**
   * determines edge label frequency and prunes by minimum support;
   * label frequencies are used to relabel edges where higher support leads
   * to a smaller numeric label;
   *
   * @return pruned and relabelled edges
   */
  @Override
  public DataSet<EdgeTriple> encode(
    GraphCollection<G, V, E> collection, FSMConfig fsmConfig) {

    setMinSupport(collection, fsmConfig);

    DataSet<VertexIdLabel> encodedVertices =
      encodeVertices(collection.getVertices());

    DataSet<EdgeTripleWithoutVertexLabels> encodedEdges =
      encodeEdges(collection.getEdges());

    DataSet<EdgeTriple> triples = combine(encodedVertices, encodedEdges);

    return frequent(triples);
  }

  private void setMinSupport(GraphCollection<G, V, E> collection,
    FSMConfig fsmConfig) {
    this.minSupport = Count
      .count(collection.getGraphHeads())
      .map(new MinSupport(fsmConfig));
  }

  private DataSet<EdgeTripleWithoutVertexLabels> encodeEdges(
    DataSet<E> edges) {
    
    edgeLabelDictionary = edges
      .flatMap(new GraphIdElementIdLabel<E>())
      .distinct()
      .map(new CountableLabel())
      .groupBy(0)
      .sum(1)
      .filter(new FrequentLabel())
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      .reduceGroup(new Dictionary());

    DataSet<Map<String, Integer>> reverseDictionary = edgeLabelDictionary
      .map(new FlipDictionary());

    return edges
      .flatMap(new EdgeLabelEncoder<E>())
      .withBroadcastSet(reverseDictionary, BroadcastNames.EDGE_DICTIONARY);
  }

  /**
   * determines vertex label frequency and prunes by minimum support;
   * label frequencies are used to relabel vertices where higher support leads
   * to a smaller numeric label;
   *
   * @param vertices input vertex collection
   * @return pruned and relabelled edges
   */
  private DataSet<VertexIdLabel> encodeVertices(DataSet<V> vertices) {

    vertexLabelDictionary = vertices
      .flatMap(new GraphIdElementIdLabel<V>())
      .distinct()
      .map(new CountableLabel())
      .groupBy(0)
      .sum(1)
      .filter(new FrequentLabel())
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
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
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      .join(triples)
      .where(0, 1, 2).equalTo(1, 2, 3)
      .with(new RightSide<LabelTripleWithSupport, EdgeTriple>());
  }

  @Override
  public DataSet<Integer> getMinSupport() {
    return minSupport;
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
