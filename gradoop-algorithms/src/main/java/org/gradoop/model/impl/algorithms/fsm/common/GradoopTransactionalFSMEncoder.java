package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.common.functions.*;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.VertexIdLabel;
import org.gradoop.model.impl.functions.tuple.Project4To0And3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;

import java.util.ArrayList;
import java.util.HashMap;

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
  private DataSet<ArrayList<String>> edgeLabelDictionary;
  /**
   * vertex label dictionary
   */
  private DataSet<ArrayList<String>> vertexLabelDictionary;

  /**
   * determines edge label frequency and prunes by minimum support;
   * label frequencies are used to relabel edges where higher support leads
   * to a smaller numeric label;
   *
   * @return pruned and relabelled edges
   */
  @Override
  public DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> encode(
    GraphCollection<G, V, E> collection, FSMConfig fsmConfig) {

    setMinSupport(collection, fsmConfig);

    DataSet<VertexIdLabel> encodedVertices = encodeVertices(collection.getVertices());

    DataSet<Tuple4<GradoopId, GradoopId, GradoopId, String>> encodedEdges =
      encodeEdges(collection.getEdges());

    return combine(encodedVertices, encodedEdges);
  }

  public void setMinSupport(GraphCollection<G, V, E> collection,
    FSMConfig fsmConfig) {
    this.minSupport = Count
      .count(collection.getGraphHeads())
      .map(new MinSupport(fsmConfig.getThreshold()));
  }

  private DataSet<Tuple4<GradoopId, GradoopId, GradoopId, String>> encodeEdges(
    DataSet<E> edges) {

    DataSet<Tuple4<GradoopId, GradoopId, GradoopId, String>> gidSidTidLabel =
      edges.flatMap(new GraphIdSourceIdTargetIdLabel<E>());

    edgeLabelDictionary = gidSidTidLabel
      .map(new Project4To0And3<GradoopId, GradoopId, GradoopId, String>())
      .distinct()
      .map(new CountableLabel())
      .groupBy(0)
      .sum(1)
      .filter(new FrequentLabel())
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      .reduceGroup(new Dictionary());

    return gidSidTidLabel;
  }

  /**
   * determines vertex label frequency and prunes by minimum support;
   * label frequencies are used to relabel vertices where higher support leads
   * to a smaller numeric label;
   *
   * @param vertices input vertex collection
   * @return pruned and relabelled edges
   */
  private DataSet<VertexIdLabel> encodeVertices(
    DataSet<V> vertices) {

    vertexLabelDictionary = vertices
      .flatMap(new GraphIdVertexIdLabel<V>())
      .distinct()
      .map(new CountableLabel())
      .groupBy(0)
      .sum(1)
      .filter(new FrequentLabel())
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      .reduceGroup(new Dictionary());

    DataSet<HashMap<String, Integer>> reverseDictionary = vertexLabelDictionary
      .map(new FlipDictionary());

    return vertices
      .flatMap(new VertexLabelEncoder<V>())
      .withBroadcastSet(reverseDictionary, VertexLabelEncoder.DICTIONARY);
  }

  private DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> combine(
    DataSet<VertexIdLabel> encodedVertices,
    DataSet<Tuple4<GradoopId, GradoopId, GradoopId, String>> encodedEdges) {

    DataSet<HashMap<String, Integer>> reverseDictionary = edgeLabelDictionary
      .map(new FlipDictionary());

    return encodedEdges
      .flatMap(new EdgeLabelEncoder())
      .withBroadcastSet(reverseDictionary, BroadcastNames.DICTIONARY)
      .join(encodedVertices).where(1).equalTo(0).with(new AppendSourceLabel())
      .join(encodedVertices).where(2).equalTo(0)
      .with(new AppendTargetLabelAndInitialDfsCode());
  }

  @Override
  public DataSet<Integer> getMinSupport() {
    return minSupport;
  }

  @Override
  public DataSet<ArrayList<String>> getEdgeLabelDictionary() {
    return edgeLabelDictionary;
  }

  @Override
  public DataSet<ArrayList<String>> getVertexLabelDictionary() {
    return vertexLabelDictionary;
  }
}
