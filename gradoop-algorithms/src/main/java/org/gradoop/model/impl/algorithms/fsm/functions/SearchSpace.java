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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledVertex;
import org.gradoop.model.impl.operators.tostring.pojos.DFSCode;
import org.gradoop.model.impl.operators.tostring.pojos.DFSEmbedding;
import org.gradoop.model.impl.operators.tostring.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * (GraphId, [Vertex,..]) |><| (GraphId, [Edge,..]) => Graph
 */
public class SearchSpace
  implements JoinFunction<
  Tuple2<GradoopId, ArrayList<IntegerLabeledVertex>>,
  Tuple2<GradoopId, ArrayList<IntegerLabeledEdge>>,
  SearchSpaceItem<Integer>
  > {

  @Override
  public SearchSpaceItem<Integer> join(
    Tuple2<GradoopId, ArrayList<IntegerLabeledVertex>> graphVertices,
    Tuple2<GradoopId, ArrayList<IntegerLabeledEdge>> graphEdges) throws
    Exception {

    HashMap<GradoopId, Integer> vertexLabels = new HashMap<>();
    HashMap<GradoopId, AdjacencyList> adjacencyLists = new HashMap<>();
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddingsMap = new
      HashMap<>();

    SearchSpaceItem<Integer> graph = SearchSpaceItem
      .createForGraph(adjacencyLists, codeEmbeddingsMap);

    for (IntegerLabeledVertex vertex : graphVertices.f1) {
      vertexLabels.put(vertex.getId(), vertex.getLabel());
      adjacencyLists.put(
        vertex.getId(), new AdjacencyList(vertex.getLabel()));
    }

    for (IntegerLabeledEdge edge : graphEdges.f1) {

      GradoopId edgeId = edge.getId();
      Integer edgeLabel = edge.getLabel();

      GradoopId sourceId = edge.getSourceId();
      Integer sourceLabel = vertexLabels.get(sourceId);

      GradoopId targetId = edge.getTargetId();
      Integer targetLabel = vertexLabels.get(targetId);

      // update adjacency lists

      adjacencyLists.get(sourceId).getEntries().add(AdjacencyListEntry
        .newOutgoing(edgeId, edgeLabel, targetId, targetLabel));

      if (!sourceId.equals(targetId)) {
        adjacencyLists.get(targetId).getEntries().add(AdjacencyListEntry
          .newIncoming(edgeId, edgeLabel, sourceId, sourceLabel));
      }

      // update code embeddings

      ArrayList<GradoopId> vertexTimes;

      Integer fromTime = 0;
      Integer fromLabel;
      Boolean outgoing;
      Integer toTime;
      Integer toLabel;

      // loop
      if (sourceId.equals(targetId)) {
        toTime = 0;

        vertexTimes = Lists.newArrayList(sourceId);

        fromLabel = sourceLabel;
        outgoing = true;
        toLabel = targetLabel;

      } else {
        toTime = 1;

        // in direction
        if (sourceLabel.compareTo(targetLabel) <= 0) {
          vertexTimes = Lists.newArrayList(sourceId, targetId);

          fromLabel = sourceLabel;
          outgoing = true;
          toLabel = targetLabel;
        } else {
          vertexTimes = Lists.newArrayList(targetId, sourceId);

          fromLabel = targetLabel;
          outgoing = false;
          toLabel = sourceLabel;
        }
      }

      DFSStep step = new DFSStep(
        fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

      CompressedDFSCode code = new CompressedDFSCode(new DFSCode(step));
      DFSEmbedding embedding = new DFSEmbedding(vertexTimes, edgeId);

      Set<DFSEmbedding> embeddings = codeEmbeddingsMap.get(code);

      if (embeddings == null) {
        codeEmbeddingsMap.put(code, Sets.newHashSet(embedding));
      } else {
        embeddings.add(embedding);
      }
    }

    return graph;
  }
}
