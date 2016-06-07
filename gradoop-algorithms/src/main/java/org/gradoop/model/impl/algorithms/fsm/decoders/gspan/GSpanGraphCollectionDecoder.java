package org.gradoop.model.impl.algorithms.fsm.decoders.gspan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.decoders.TransactionalFSMDecoder;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.functions.DfsDecoder;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.functions.EdgeLabelDecoder;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.functions.ExpandEdges;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.functions.ExpandVertices;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.functions.FullEdge;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.functions.FullVertex;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.functions.VertexLabelDecoder;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.List;


public class GSpanGraphCollectionDecoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements TransactionalFSMDecoder<GraphCollection<G, V, E>> {

  private final GradoopFlinkConfig<G, V, E> gradoopConfig;

  public GSpanGraphCollectionDecoder(
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig) {

    this.gradoopConfig = gradoopFlinkConfig;
  }

  /**
   * turns a data set of DFS codes into a graph collection
   * @return graph collection
   */
  @Override
  public GraphCollection<G, V, E> decode(
    DataSet<WithCount<CompressedSubgraph>> codes,
    DataSet<List<String>> vertexLabelDictionary,
    DataSet<List<String>> edgeLabelDictionary) {


    DataSet<Tuple3<G, ArrayList<Tuple2<GradoopId, Integer>>,
      ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>> frequentSubgraphs =
      codes
        .map(new DfsDecoder<>(gradoopConfig.getGraphHeadFactory()));

    DataSet<G> graphHeads = frequentSubgraphs
      .map(new Value0Of3<G, ArrayList<Tuple2<GradoopId, Integer>>,
        ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>());

    DataSet<V> vertices = frequentSubgraphs
      .flatMap(new ExpandVertices<G>())
      .map(new VertexLabelDecoder())
      .withBroadcastSet(vertexLabelDictionary, BroadcastNames.VERTEX_DICTIONARY)
      .map(new FullVertex<>(gradoopConfig.getVertexFactory()))
      .returns(gradoopConfig.getVertexFactory().getType());

    DataSet<E> edges = frequentSubgraphs
      .flatMap(new ExpandEdges<G>())
      .map(new EdgeLabelDecoder())
      .withBroadcastSet(edgeLabelDictionary, BroadcastNames.EDGE_DICTIONARY)
      .map(new FullEdge<>(gradoopConfig.getEdgeFactory()))
      .returns(gradoopConfig.getEdgeFactory().getType());

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, gradoopConfig);
  }
}
