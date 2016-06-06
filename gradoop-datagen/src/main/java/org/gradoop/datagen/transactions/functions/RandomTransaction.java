/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.datagen.transactions.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.datagen.transactions.FSMTransactionGeneratorConfig;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * graphIndex >< graphCount => graphTransaction
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class RandomTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CrossFunction<Long, Long, GraphTransaction<G, V, E>>,
  ResultTypeQueryable<GraphTransaction<G, V, E>> {

  /**
   * configuration
   */
  private final FSMTransactionGeneratorConfig config;
  /**
   * graph head factory
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;
  /**
   * vertex factory
   */
  private final EPGMVertexFactory<V> vertexFactory;
  /**
   * edge factory
   */
  private final EPGMEdgeFactory<E> edgeFactory;
  /**
   * vertex labels
   */
  private final List<String> vertexLabels;
  /**
   * edge labels
   */
  private final List<String> edgeLabels;

  /**
   * constructor
   * @param gradoopFlinkConfig Gradoop configuration
   * @param generatorConfig generator configuration
   * @param vertexLabels vertex labels
   * @param edgeLabels edge labels
   */
  public RandomTransaction(
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

  /**
   * creates edges
   * @param vertexCount number of vertices
   * @param edgeCount number of edges
   * @param graphIds graph ids for the created vertices
   * @param vertexIds vertex ids
   * @param edges empty set of edges
   */
  public void createEdges(int vertexCount, int edgeCount, GradoopIdSet graphIds,
    List<GradoopId> vertexIds, Set<E> edges) {
    Map<Integer, Collection<Integer>> sourceTargets =
      getSourceTargetIndices(vertexCount, edgeCount);

    int edgeIndex = 0;
    int edgeLabelCount = edgeLabels.size();

    for (Map.Entry<Integer, Collection<Integer>> entry :
      sourceTargets.entrySet()) {

      int sourceIndex = entry.getKey();
      GradoopId sourceId = vertexIds.get(sourceIndex);

      Collection<Integer> targetIndices = entry.getValue();

      for (int targetIndex : targetIndices) {
        GradoopId targetId = vertexIds.get(targetIndex);

        String label = edgeLabels.get(edgeIndex % edgeLabelCount);

        edges.add(edgeFactory.createEdge(label, sourceId, targetId, graphIds));
      }
      edgeIndex++;
    }
  }

  /**
   * creates vertices
   * @param vertexCount number of vertices to create
   * @param graphIds graph ids for the created vertices
   * @param vertexIds list of vertex ids
   * @param vertices empty set of vertices
   */
  public void createVertices(int vertexCount, GradoopIdSet graphIds,
    List<GradoopId> vertexIds, Set<V> vertices) {
    for (int i = 0; i < vertexCount; i++) {
      vertexIds.add(GradoopId.get());
    }

    int vertexIndex = 0;
    int vertexLabelCount = vertexLabels.size();

    for (GradoopId vertexId : vertexIds) {
      String label = vertexLabels.get(vertexIndex % vertexLabelCount);
      vertices.add(vertexFactory.initVertex(vertexId, label, graphIds));
      vertexIndex++;
    }
  }

  /**
   * links source and target vertices by numeric index
   * @param vertexCount number of vertices
   * @param edgeCount number of edges
   * @return index adjacency lists
   */
  public Map<Integer, Collection<Integer>> getSourceTargetIndices(
    int vertexCount, int edgeCount) {
    Map<Integer, Collection<Integer>> sourceTargets =
      Maps.newHashMapWithExpectedSize(vertexCount);

    sourceTargets.put(1, Lists.newArrayList(2));

    Random random = new Random();

    int maxIndex = vertexCount - 1;
    int maxConnectedIndex = 1;

    for (int i = 1; i < edgeCount; i++) {

      int sourceIndex;
      int targetIndex;

      if (maxConnectedIndex < vertexCount) {
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
