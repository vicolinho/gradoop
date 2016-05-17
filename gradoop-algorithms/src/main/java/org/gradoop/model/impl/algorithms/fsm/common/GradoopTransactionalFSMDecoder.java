package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMDecoder;
import org.gradoop.model.impl.algorithms.fsm.common.functions.DfsDecoder;
import org.gradoop.model.impl.algorithms.fsm.common.functions.EdgeLabelDecoder;
import org.gradoop.model.impl.algorithms.fsm.common.functions.ExpandEdges;
import org.gradoop.model.impl.algorithms.fsm.common.functions.ExpandVertices;
import org.gradoop.model.impl.algorithms.fsm.common.functions.FullEdge;
import org.gradoop.model.impl.algorithms.fsm.common.functions.FullVertex;
import org.gradoop.model.impl.algorithms.fsm.common.functions.VertexLabelDecoder;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.ArrayList;

/**
 * Created by peet on 17.05.16.
 */
public class GradoopTransactionalFSMDecoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements TransactionalFSMDecoder<GraphCollection<G, V, E>> {

  private final GradoopFlinkConfig<G, V, E> gradoopConfig;

  public GradoopTransactionalFSMDecoder(
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig) {

    this.gradoopConfig = gradoopFlinkConfig;
  }

  /**
   * turns a data set of DFS codes into a graph collection
   * @param frequentDfsCodes DFS code data set
   * @return graph collection
   */
  @Override
  public GraphCollection<G, V, E> decode(
    DataSet<CompressedDFSCode> frequentDfsCodes,
    DataSet<ArrayList<String>> vertexLabelDictionary,
    DataSet<ArrayList<String>> edgeLabelDictionary) {


    DataSet<Tuple3<G, ArrayList<Tuple2<GradoopId, Integer>>,
      ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>> frequentSubgraphs =
      frequentDfsCodes
        .map(new DfsDecoder<>(gradoopConfig.getGraphHeadFactory()));

    DataSet<G> graphHeads = frequentSubgraphs
      .map(new Value0Of3<G, ArrayList<Tuple2<GradoopId, Integer>>,
        ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>());

    DataSet<V> vertices = frequentSubgraphs
      .flatMap(new ExpandVertices<G>())
      .map(new VertexLabelDecoder())
      .withBroadcastSet(vertexLabelDictionary, BroadcastNames.DICTIONARY)
      .map(new FullVertex<>(gradoopConfig.getVertexFactory()))
      .returns(gradoopConfig.getVertexFactory().getType());

    DataSet<E> edges = frequentSubgraphs
      .flatMap(new ExpandEdges<G>())
      .map(new EdgeLabelDecoder())
      .withBroadcastSet(edgeLabelDictionary, BroadcastNames.DICTIONARY)
      .map(new FullEdge<>(gradoopConfig.getEdgeFactory()))
      .returns(gradoopConfig.getEdgeFactory().getType());

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, gradoopConfig);
  }
}
