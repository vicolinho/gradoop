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
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSStep;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * CompressedDFSCode => (Graph, Vertex, Edge)
 * @param <G> graph type
 */
public class DfsDecoder<G extends EPGMGraphHead> implements
  ResultTypeQueryable
  <Tuple3<G, ArrayList<IntegerLabeledVertex>, ArrayList<IntegerLabeledEdge>>>,
  MapFunction<CompressedDFSCode, 
    Tuple3<G, ArrayList<IntegerLabeledVertex>, ArrayList<IntegerLabeledEdge>>> {

  /**
   * graph head factory
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;
//  /**
//   * vertex factory
//   */
//  private final EPGMVertexFactory<IntegerLabeledVertex> vertexFactory;
//  /**
//   * edge factory
//   */
//  private final EPGMEdgeFactory<IntegerLabeledEdge> edgeFactory;

  /**
   * constructor
   * @param graphHeadFactory graph head factory
   */
  public DfsDecoder(EPGMGraphHeadFactory<G> graphHeadFactory) {

    this.graphHeadFactory = graphHeadFactory;

//    this.vertexFactory = vertexFactory;
//    this.edgeFactory = edgeFactory;
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
    Integer label,
    ArrayList<IntegerLabeledVertex> vertices,
    Map<Integer, GradoopId> timeIdMap,
    GradoopIdSet graphIds
  ) {

    GradoopId id = timeIdMap.get(time);

    if (id == null) {
      IntegerLabeledVertex vertex = new IntegerLabeledVertex();

      vertex.setId(GradoopId.get());
      vertex.setLabel(label);

      id = vertex.getId();
      vertices.add(vertex);
      timeIdMap.put(time, id);
    }
    return id;
  }


  @Override
  public Tuple3<G, ArrayList<IntegerLabeledVertex>, ArrayList<IntegerLabeledEdge>> map(
    CompressedDFSCode compressedDfsCode) throws  Exception {

//    System.out.println("decode " + compressedDfsCode);

    DFSCode dfsCode = compressedDfsCode.getDfsCode();

    G graphHead = graphHeadFactory.createGraphHead();

    graphHead.setProperty("dfsCode", dfsCode.toString());
    graphHead.setProperty("support", compressedDfsCode.getSupport());

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    ArrayList<IntegerLabeledVertex> vertices = Lists.newArrayList();

    ArrayList<IntegerLabeledEdge> edges = Lists
      .newArrayListWithCapacity(dfsCode.getSteps().size());

    Map<Integer, GradoopId> vertexTimeId = new HashMap<>();

    for (DFSStep step : dfsCode.getSteps()) {

      Integer fromTime = step.getFromTime();
      Integer fromLabel = step.getFromLabel();

      Integer toTime = step.getToTime();
      Integer toLabel = step.getToLabel();

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

      IntegerLabeledEdge integerLabeledEdge = new IntegerLabeledEdge();

      integerLabeledEdge.setId(GradoopId.get());
      integerLabeledEdge.setSourceId(sourceId);
      integerLabeledEdge.setTargetId(targetId);
      integerLabeledEdge.setLabel(step.getEdgeLabel());

      edges.add(integerLabeledEdge);
    }

    return new Tuple3<>(graphHead, vertices, edges);
  }

  @Override
  public TypeInformation<Tuple3<G, ArrayList<IntegerLabeledVertex>, ArrayList<IntegerLabeledEdge>>>
  getProducedType() {
    return new TupleTypeInfo<>(
      TypeExtractor.getForClass(graphHeadFactory.getType()),
      TypeExtractor.getForClass(ArrayList.class),
      TypeExtractor.getForClass(ArrayList.class)
    );
  }
}
