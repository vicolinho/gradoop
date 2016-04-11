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
import java.util.Random;
import java.util.Set;

public class FSMTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements ResultTypeQueryable<GraphTransaction<G, V, E>>,
  CrossFunction<Long, Long, GraphTransaction<G, V, E>> {

  private final EPGMGraphHeadFactory<G> graphHeadFactory;
  private final FSMTransactionGeneratorConfig config;
  private final EPGMVertexFactory<V> vertexFactory;
  private final EPGMEdgeFactory<E> edgeFactory;
  private final List<String> vertexLabels;
  private final List<String> edgeLabels;

  public FSMTransaction(
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FSMTransactionGeneratorConfig generatorConfig,
    List<String> vertexLabels,
    List<String> edgeLabels) {

    this.graphHeadFactory = gradoopFlinkConfig.getGraphHeadFactory();
    this.vertexFactory = gradoopFlinkConfig.getVertexFactory();
    this.edgeFactory = gradoopFlinkConfig.getEdgeFactory();
    this.config = generatorConfig;
    this.vertexLabels = vertexLabels;
    this.edgeLabels = edgeLabels;
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

    int vertexCount = config.calculateVertexCount(x, n);
    int edgeCount = config.calculateEdgeCount(x, n);

    // create graph head
    G graphHead = graphHeadFactory.createGraphHead(x.toString());
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    // create vertices
    List<GradoopId> vertexIds = Lists.newArrayListWithCapacity(vertexCount);
    Set<V> vertices = Sets.newHashSetWithExpectedSize(vertexCount);
    createVertices(vertexCount, graphIds, vertexIds, vertices);

    // create edges
    Set<E> edges =  Sets.newHashSetWithExpectedSize(edgeCount);
    createEdges(vertexCount, edgeCount, graphIds, vertexIds, edges);

    return new GraphTransaction<>(graphHead, vertices, edges);
  }

  public void createEdges(int vertexCount, int edgeCount, GradoopIdSet graphIds,
    List<GradoopId> vertexIds, Set<E> edges) {
    Map<Integer, Collection<Integer>> sourceTargets =
      getSourceTargetIndices(vertexCount, edgeCount);

    int edgeIndex = 0;
    int edgeLabelCount = edgeLabels.size();

    for(Map.Entry<Integer, Collection<Integer>> entry :
      sourceTargets.entrySet()) {

      int sourceIndex = entry.getKey();
      GradoopId sourceId = vertexIds.get(sourceIndex);

      Collection<Integer> targetIndices = entry.getValue();

      for(int targetIndex : targetIndices) {
        GradoopId targetId = vertexIds.get(targetIndex);

        String label = edgeLabels.get(edgeIndex % edgeLabelCount);

        edges.add(edgeFactory.createEdge(label, sourceId, targetId, graphIds));
      }
      edgeIndex++;
    }
  }

  public void createVertices(int vertexCount, GradoopIdSet graphIds,
    List<GradoopId> vertexIds, Set<V> vertices) {
    for(int i = 0; i < vertexCount; i++) {
      vertexIds.add(GradoopId.get());
    }

    int vertexIndex = 0;
    int vertexLabelCount = vertexLabels.size();

    for(GradoopId vertexId : vertexIds) {
      String label = vertexLabels.get(vertexIndex % vertexLabelCount);
      vertices.add(vertexFactory.initVertex(vertexId, label, graphIds));
      vertexIndex++;
    }
  }

  public Map<Integer, Collection<Integer>> getSourceTargetIndices(
    int vertexCount, int edgeCount) {
    Map<Integer, Collection<Integer>> sourceTargets =
      Maps.newHashMapWithExpectedSize(vertexCount);

    sourceTargets.put(1, Lists.newArrayList(2));

    Random random = new Random();

    int maxIndex = vertexCount - 1;
    int maxConnectedIndex = 1;

    for(int i = 1; i < edgeCount; i++) {

      int sourceIndex;
      int targetIndex;

      if(maxConnectedIndex < vertexCount) {
        sourceIndex = maxConnectedIndex;
        targetIndex = random.nextInt(maxConnectedIndex);
        maxConnectedIndex++;
      } else {
        sourceIndex = random.nextInt(maxIndex);
        targetIndex = random.nextInt(maxIndex);
      }

      Collection<Integer> targetIndices = sourceTargets.get(sourceIndex);

      if (targetIndices == null) {
        sourceTargets.put(sourceIndex, Lists.newArrayList(targetIndex));
      } else {
        targetIndices.add(targetIndex);
      }
    }
    return sourceTargets;
  }
}
