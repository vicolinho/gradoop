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

package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.operators.tostring.pojos.DFSCode;
import org.gradoop.model.impl.operators.tostring.pojos.DFSStep;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * CompressedDFSCode => (Graph, Vertex, Edge)
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class DfsDecoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements ResultTypeQueryable<Tuple3<G, Collection<V>, Collection<E>>>,
  MapFunction<CompressedDFSCode, Tuple3<G, Collection<V>, Collection<E>>> {

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
   * constructor
   * @param graphHeadFactory graph head factory
   * @param vertexFactory vertex factory
   * @param edgeFactory edge factory
   */
  public DfsDecoder(
    EPGMGraphHeadFactory<G> graphHeadFactory,
    EPGMVertexFactory<V> vertexFactory,
    EPGMEdgeFactory<E> edgeFactory) {

    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  /**
   * returns the vertex id of a given time
   * or creates a new vertex if none exists.
   * @param time vertex time
   * @param label vertex label
   * @param vertices vertices
   * @param timeIdMap mapping : vertex time => vertex id
   * @param graphIds set with one graph id
   * @return vertex id
   */
  protected GradoopId getOrCreateVertex(
    Integer time,
    String label,
    Collection<V> vertices,
    Map<Integer, GradoopId> timeIdMap,
    GradoopIdSet graphIds
  ) {

    GradoopId id = timeIdMap.get(time);

    if (id == null) {
      V vertex = vertexFactory
        .createVertex(label, graphIds);

      id = vertex.getId();
      vertices.add(vertex);
      timeIdMap.put(time, id);
    }
    return id;
  }


  @Override
  public Tuple3<G, Collection<V>, Collection<E>> map(
    CompressedDFSCode compressedDfsCode) throws  Exception {

//    System.out.println("decode " + compressedDfsCode);

    DFSCode dfsCode = compressedDfsCode.getDfsCode();

    G graphHead = graphHeadFactory.createGraphHead();

    graphHead.setProperty("dfsCode", dfsCode.toString());
    graphHead.setProperty("support", compressedDfsCode.getSupport());

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Collection<V> vertices = Lists.newArrayList();

    Collection<E> edges = Lists
      .newArrayListWithCapacity(dfsCode.getSteps().size());

    Map<Integer, GradoopId> vertexTimeId = new HashMap<>();

    for (DFSStep step : dfsCode.getSteps()) {

      Integer fromTime = step.getFromTime();
      String fromLabel = step.getFromLabel();

      Integer toTime = step.getToTime();
      String toLabel = step.getToLabel();

      GradoopId targetId;
      GradoopId sourceId;

      if (step.isOutgoing()) {
        sourceId = getOrCreateVertex(
          fromTime, fromLabel, vertices, vertexTimeId, graphIds);

        targetId = getOrCreateVertex(
          toTime, toLabel, vertices, vertexTimeId, graphIds);

      } else {
        sourceId = getOrCreateVertex(
          toTime, toLabel, vertices, vertexTimeId, graphIds);

        if (step.isLoop()) {
          targetId = sourceId;
        } else {
          targetId = getOrCreateVertex(
            fromTime, fromLabel, vertices, vertexTimeId, graphIds);
        }
      }

      edges.add(edgeFactory.createEdge(
        step.getEdgeLabel(), sourceId, targetId, graphIds));
    }

    return new Tuple3<>(graphHead, vertices, edges);
  }

  @Override
  public TypeInformation<Tuple3<G, Collection<V>, Collection<E>>>
  getProducedType() {
    return new TupleTypeInfo<>(
      TypeExtractor.getForClass(graphHeadFactory.getType()),
      TypeExtractor.getForClass(Collection.class),
      TypeExtractor.getForClass(Collection.class));
  }
}
