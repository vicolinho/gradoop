package org.gradoop.datagen.fsmtransactions.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.datagen.fsmtransactions.FSMTransactionGeneratorConfig;
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
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FSMTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements ResultTypeQueryable<GraphTransaction<G, V, E>>,
  CrossFunction<Long, Long, GraphTransaction<G, V, E>> {

  private final EPGMGraphHeadFactory<G> graphHeadFactory;
  private final FSMTransactionGeneratorConfig config;
  private final EPGMVertexFactory<V> vertexFactory;
  private final EPGMEdgeFactory<E> edgeFactory;

  public FSMTransaction(GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FSMTransactionGeneratorConfig generatorConfig) {

    this.graphHeadFactory = gradoopFlinkConfig.getGraphHeadFactory();
    this.vertexFactory = gradoopFlinkConfig.getVertexFactory();
    this.edgeFactory = gradoopFlinkConfig.getEdgeFactory();
    this.config = generatorConfig;
  }

  @Override
  public TypeInformation<GraphTransaction<G, V, E>> getProducedType() {
    return new TupleTypeInfo<>(
      TypeExtractor.getForClass(graphHeadFactory.getType()),
      TypeExtractor.getForClass(Set.class),
      TypeExtractor.getForClass(Set.class));
  }

  @Override
  public GraphTransaction<G, V, E> cross(Long x, Long n) throws Exception {

    // create graph
    GraphTransaction<G, V, E> graph = new GraphTransaction<>();
    G graphHead = graphHeadFactory.createGraphHead(x.toString());
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    // create vertices

    int vertexCount = config.calculateVertexCount(x, n);
    int edgeCount = config.calculateEdgeCount(x, n);

    Collection<Integer> vertexIndices = Lists.newArrayListWithCapacity(vertexCount);
    List<GradoopId> vertexIds = Lists.newArrayListWithCapacity(vertexCount);

    Set<Integer> connectedVertexIndices = Sets
      .newHashSetWithExpectedSize(vertexCount);

    Map<Integer, Collection<Integer>> sourceTargets =
      Maps.newHashMapWithExpectedSize(vertexCount);

    for(int i = 0; i < vertexCount; i++) {
      vertexIndices.add(i);
      vertexIds.add(GradoopId.get());
    }

    sourceTargets.put(1, Lists.newArrayList(2));
    connectedVertexIndices.add(1);
    connectedVertexIndices.add(2);

    Iterator<Integer> sourceIterator = connectedVertexIndices.iterator();
    Iterator<Integer> targetIterator = vertexIndices.iterator();

    for(int i = 2; i< edgeCount; i++) {
      if(! sourceIterator.hasNext()) {
        sourceIterator = connectedVertexIndices.iterator();
      }
      if(! targetIterator.hasNext()) {
        targetIterator = connectedVertexIndices.iterator();
      }

      int sourceIndex = sourceIterator.next();
      int targetIndex = targetIterator.next();

      Collection<Integer> targetIndices = sourceTargets.get(sourceIndex);

      if (targetIndices == null) {
        sourceTargets.put(sourceIndex, Lists.newArrayList(targetIndex));
      } else {
        targetIndices.add(targetIndex);
      }
    }

    Set<V> vertices = Sets.newHashSetWithExpectedSize(vertexCount);

    for(GradoopId vertexId : vertexIds) {
      String label = "A";
      vertices.add(vertexFactory.initVertex(vertexId, label, graphIds));
    }

    Set<E> edges =  Sets.newHashSetWithExpectedSize(edgeCount);

    for(Map.Entry<Integer, Collection<Integer>> entry :
      sourceTargets.entrySet()) {

      int sourceIndex = entry.getKey();
      GradoopId sourceId = vertexIds.get(sourceIndex);

      Collection<Integer> targetIndices = entry.getValue();

      for(int targetIndex : targetIndices) {
        GradoopId targetId = vertexIds.get(targetIndex);

        String label = "a";

        edges.add(edgeFactory.createEdge(label, sourceId, targetId, graphIds));
      }
    }

    return new GraphTransaction<>(graphHead, vertices, edges);  }
}
