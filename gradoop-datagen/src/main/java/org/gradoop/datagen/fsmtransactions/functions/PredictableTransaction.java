package org.gradoop.datagen.fsmtransactions.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.GraphTransaction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.ArrayList;
import java.util.Set;

public class PredictableTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<Long, GraphTransaction<G, V, E>>
  , ResultTypeQueryable<GraphTransaction<G, V, E>> {

  private final ArrayList<String> vertexLabels = Lists
    .newArrayList("A", "B", "C", "D", "E", "F", "G", "H", "J", "K");
  private final EPGMGraphHeadFactory<G> graphHeadFactory;
  private final EPGMVertexFactory<V> vertexFactory;
  private final EPGMEdgeFactory<E> edgeFactory;


  public PredictableTransaction(
    EPGMGraphHeadFactory<G> graphHeadFactory,
    EPGMVertexFactory<V> vertexFactory,
    EPGMEdgeFactory<E> edgeFactory) {

    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

  }

  @Override
  public GraphTransaction<G, V, E> map(Long seed) throws Exception {

    Long minSupport = seed % 10;

    G graphHead = graphHeadFactory
      .createGraphHead(String.valueOf(minSupport + 1));

    Set<V> vertices = Sets.newHashSet();
    Set<E> edges = Sets.newHashSet();

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    V seedVertex = vertexFactory.createVertex("S", graphIds);
    vertices.add(seedVertex);

    for(int i = 0; i <= minSupport; i++) {
      String vertexLabel = vertexLabels.get(i);

      addPattern(vertexLabel, seedVertex, vertices, edges);
    }

    for(V vertex : vertices) {
      vertex.setGraphIds(graphIds);
    }
    for(E edge : edges) {
      edge.setGraphIds(graphIds);
    }

    return new GraphTransaction<>(graphHead, vertices, edges);
  }

  private void addPattern(
    String vertexLabel, V seedVertex, Set<V> vertices, Set<E> edges) {

    // parallel edges and loop
    GradoopId multiBottomId = createVertex(vertexLabel, vertices);
    GradoopId multiTopId = createVertex(vertexLabel, vertices);

    createEdge(multiBottomId, "p", multiTopId, edges);
    createEdge(multiTopId, "p", multiBottomId, edges);
    createEdge(multiTopId, "p", multiBottomId, edges);
    createEdge(multiTopId, "l", multiTopId, edges);

    createEdge(
      seedVertex.getId(), multiBottomId.toString(), multiBottomId, edges);

    // mirror

    GradoopId mirrorBottomId = createVertex(vertexLabel, vertices);
    GradoopId mirrorTopId = createVertex(vertexLabel, vertices);
    GradoopId mirrorLeftId = createVertex(vertexLabel, vertices);
    GradoopId mirrorRightId = createVertex(vertexLabel, vertices);

    createEdge(mirrorBottomId, "m", mirrorLeftId, edges);
    createEdge(mirrorBottomId, "m", mirrorRightId, edges);
    createEdge(mirrorLeftId, "m", mirrorTopId, edges);
    createEdge(mirrorRightId, "m", mirrorTopId, edges);

    createEdge(mirrorBottomId, "s", multiBottomId, edges);

    // cycle

    GradoopId cycleBottomId = createVertex(vertexLabel, vertices);
    GradoopId cycleLeftId = createVertex(vertexLabel, vertices);
    GradoopId cycleRightId = createVertex(vertexLabel, vertices);

    createEdge(cycleBottomId, "c", cycleLeftId, edges);
    createEdge(cycleLeftId, "c", cycleRightId, edges);
    createEdge(cycleRightId, "c", cycleBottomId, edges);

    createEdge(cycleBottomId, "s", multiBottomId, edges);
  }

  private void createEdge(
    GradoopId sourceId, String label, GradoopId targetId, Set<E> edges) {

    edges.add(edgeFactory.createEdge(label, sourceId, targetId));
  }

  private GradoopId createVertex(String vertexLabel, Set<V> vertices) {

    V vertex = vertexFactory.createVertex(vertexLabel);
    vertices.add(vertex);
    return vertex.getId();
  }

  @Override
  public TypeInformation<GraphTransaction<G, V, E>> getProducedType() {
    return new TupleTypeInfo<>(
      TypeExtractor.getForClass(graphHeadFactory.getType()),
      TypeExtractor.getForClass(Set.class),
      TypeExtractor.getForClass(Set.class));
  }
}
