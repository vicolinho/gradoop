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
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * (GraphId, [Vertex,..]) |><| (GraphId, [Edge,..]) => Graph
 */
public class IterativeSearchSpace implements GroupReduceFunction
  <Tuple3<GradoopId, FatEdge, CompressedDFSCode>, SearchSpaceItem> {

  public SearchSpaceItem join(
    Tuple2<GradoopId, ArrayList<Tuple2<GradoopId, Integer>>> graphVertices,
    Tuple2<GradoopId, ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>
      graphEdges) throws Exception {

    HashMap<GradoopId, Integer> vertexIndexMap = new HashMap<>();
    ArrayList<AdjacencyList> adjacencyLists = new ArrayList<>();
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddingsMap = new
      HashMap<>();

    SearchSpaceItem graph = SearchSpaceItem
      .createForGraph(adjacencyLists, codeEmbeddingsMap);

    int vertexIndex = 0;
    for (Tuple2<GradoopId, Integer> vertex : graphVertices.f1) {
      vertexIndexMap.put(vertex.f0, vertexIndex);
      adjacencyLists.add(new AdjacencyList(vertex.f1));
      vertexIndex++;
    }

    int edgeIndex = 0;
    for (Tuple3<GradoopId, GradoopId, Integer> edge : graphEdges.f1) {

      Integer sourceIndex = vertexIndexMap.get(edge.f0);

      // source vertex has frequent label
      if (sourceIndex != null) {

        Integer targetIndex = vertexIndexMap.get(edge.f1);

        // target vertex has frequent label
        if (targetIndex != null) {
          Integer sourceLabel = adjacencyLists
            .get(sourceIndex).getVertexLabel();
          Integer targetLabel = adjacencyLists
            .get(targetIndex).getVertexLabel();

          Integer edgeLabel = edge.f2;

          // update adjacency lists

          adjacencyLists.get(sourceIndex).getEntries().add(AdjacencyListEntry
            .newOutgoing(edgeIndex, edgeLabel, targetIndex, targetLabel));

          if (!sourceIndex.equals(targetIndex)) {
            adjacencyLists.get(targetIndex).getEntries().add(AdjacencyListEntry
              .newIncoming(edgeIndex, edgeLabel, sourceIndex, sourceLabel));
          }

          // update code embeddings

          ArrayList<Integer> vertexTimes;

          Integer fromTime = 0;
          Integer fromLabel;
          Boolean outgoing;
          Integer toTime;
          Integer toLabel;

          // loop
          if (sourceIndex.equals(targetIndex)) {
            toTime = 0;

            vertexTimes = Lists.newArrayList(sourceIndex);

            fromLabel = sourceLabel;
            outgoing = true;
            toLabel = targetLabel;

          } else {
            toTime = 1;

            // in direction
            if (sourceLabel.compareTo(targetLabel) <= 0) {
              vertexTimes = Lists.newArrayList(sourceIndex, targetIndex);

              fromLabel = sourceLabel;
              outgoing = true;
              toLabel = targetLabel;
            } else {
              vertexTimes = Lists.newArrayList(targetIndex, sourceIndex);

              fromLabel = targetLabel;
              outgoing = false;
              toLabel = sourceLabel;
            }
          }

          DFSStep step = new DFSStep(
            fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

          CompressedDFSCode code = new CompressedDFSCode(new DFSCode(step));
          DFSEmbedding embedding = new DFSEmbedding(vertexTimes, edgeIndex);

          Set<DFSEmbedding> embeddings = codeEmbeddingsMap.get(code);

          if (embeddings == null) {
            codeEmbeddingsMap.put(code, Sets.newHashSet(embedding));
          } else {
            embeddings.add(embedding);
          }
        }
      }
      edgeIndex++;
    }

    return graph;
  }

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    Collector<SearchSpaceItem> collector) throws Exception {

    HashMap<GradoopId, Integer> vertexIndexMap = new HashMap<>();
    ArrayList<AdjacencyList> adjacencyLists = new ArrayList<>();
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddingsMap = new
      HashMap<>();

    SearchSpaceItem graph = SearchSpaceItem
      .createForGraph(adjacencyLists, codeEmbeddingsMap);

    int vertexIndex = 0;
    int edgeIndex = 0;

    for(Tuple3<GradoopId, FatEdge, CompressedDFSCode> triple : iterable) {

      FatEdge edge = triple.f1;
      CompressedDFSCode code = triple.f2;

      Integer edgeLabel = edge.getLabel();

      // get or create source vertex adjacency list

      GradoopId sourceId = edge.getSourceId();
      Integer sourceLabel = edge.getSourceLabel();
      Integer sourceIndex = vertexIndexMap.get(sourceId);

      AdjacencyList sourceAdjacencyList;

      if(sourceIndex == null) {
        sourceIndex = vertexIndex;
        vertexIndexMap.put(sourceId, sourceIndex);
        vertexIndex++;

        sourceAdjacencyList = new AdjacencyList(sourceLabel);
        adjacencyLists.add(sourceAdjacencyList);
      } else {
        sourceAdjacencyList = adjacencyLists.get(sourceIndex);
      }

      // get or create target vertex adjacency list

      GradoopId targetId = edge.getTargetId();
      Integer targetLabel = edge.getTargetLabel();
      Integer targetIndex;
      AdjacencyList targetAdjacencyList;

      if(sourceId.equals(targetId)) {
        targetIndex = sourceIndex;
        targetAdjacencyList = sourceAdjacencyList;
      } else {
        targetIndex = vertexIndexMap.get(targetId);

        if(targetIndex == null) {
          targetIndex = vertexIndex;
          vertexIndexMap.put(targetId, targetIndex);
          vertexIndex++;

          targetAdjacencyList = new AdjacencyList(targetLabel);
          adjacencyLists.add(targetAdjacencyList);
        } else {
          targetAdjacencyList = adjacencyLists.get(targetIndex);
        }
      }

      // update adjacency lists
      sourceAdjacencyList.getEntries().add(AdjacencyListEntry
        .newOutgoing(edgeIndex, edgeLabel, targetIndex, targetLabel));

      targetAdjacencyList.getEntries().add(AdjacencyListEntry
        .newIncoming(edgeIndex, edgeLabel, sourceIndex, sourceLabel));


      // update code embeddings
      ArrayList<Integer> vertexTimes;

      // loop
      if (sourceIndex.equals(targetIndex)) {
        vertexTimes = Lists.newArrayList(sourceIndex);
      } else {
        if (sourceLabel.compareTo(targetLabel) <= 0) {
          vertexTimes = Lists.newArrayList(sourceIndex, targetIndex);
        } else {
          vertexTimes = Lists.newArrayList(targetIndex, sourceIndex);
        }
      }

      DFSEmbedding embedding = new DFSEmbedding(vertexTimes, edgeIndex);

      Set<DFSEmbedding> embeddings = codeEmbeddingsMap.get(code);

      if (embeddings == null) {
        codeEmbeddingsMap.put(code, Sets.newHashSet(embedding));
      } else {
        embeddings.add(embedding);
      }

      edgeIndex++;
    }

    collector.collect(graph);
  }
}
