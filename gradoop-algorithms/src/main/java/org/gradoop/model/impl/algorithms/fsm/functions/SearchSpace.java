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
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledEdge;
import org.gradoop.model.impl.operators.tostring.pojos.DFSCode;
import org.gradoop.model.impl.operators.tostring.pojos.DFSEmbedding;
import org.gradoop.model.impl.operators.tostring.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * (GraphId, [Vertex,..]) |><| (GraphId, [Edge,..]) => Graph
 */
public class SearchSpace<L extends Comparable<L>>
  implements JoinFunction<
  Tuple2<GradoopId, Collection<LabeledVertex<L>>>,
  Tuple2<GradoopId, Collection<LabeledEdge<L>>>,
  SearchSpaceItem<L>
  > {

  @Override
  public SearchSpaceItem<L> join(
    Tuple2<GradoopId, Collection<LabeledVertex<L>>> graphVertices,
    Tuple2<GradoopId, Collection<LabeledEdge<L>>> graphEdges) throws
    Exception {

    GradoopId graphId = graphVertices.f0;
    HashMap<GradoopId, L> vertexLabels = new HashMap<>();
    HashMap<GradoopId, AdjacencyList<L>> adjacencyLists = new HashMap<>();
    HashMap<CompressedDFSCode<L>, HashSet<DFSEmbedding>> codeEmbeddingsMap = new
      HashMap<>();

    SearchSpaceItem<L> graph = SearchSpaceItem.createForGraph(adjacencyLists, codeEmbeddingsMap);

    for (LabeledVertex<L> vertex : graphVertices.f1) {
      vertexLabels.put(vertex.getId(), vertex.getLabel());
      adjacencyLists.put(
        vertex.getId(), new AdjacencyList<L>(vertex.getLabel()));
    }

    for (LabeledEdge<L> edge : graphEdges.f1) {

      GradoopId edgeId = edge.getId();
      L edgeLabel = edge.getLabel();

      GradoopId sourceId = edge.getSourceId();
      L sourceLabel = vertexLabels.get(sourceId);

      GradoopId targetId = edge.getTargetId();
      L targetLabel = vertexLabels.get(targetId);

      // update adjacency lists

      adjacencyLists.get(sourceId).getEntries().add(AdjacencyListEntry
        .<L>newOutgoing(edgeId, edgeLabel, targetId, targetLabel));

      if (!sourceId.equals(targetId)) {
        adjacencyLists.get(targetId).getEntries().add(AdjacencyListEntry
          .newIncoming(edgeId, edgeLabel, sourceId, sourceLabel));
      }

      // update code embeddings

      ArrayList<GradoopId> vertexTimes;

      Integer fromTime = 0;
      L fromLabel;
      Boolean outgoing;
      Integer toTime;
      L toLabel;

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

      DFSStep<L> step = new DFSStep<>(
        fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

      CompressedDFSCode<L> code = new CompressedDFSCode<L>(new DFSCode(step));
      DFSEmbedding embedding = new DFSEmbedding(vertexTimes, edgeId);

      Collection<DFSEmbedding> embeddings = codeEmbeddingsMap.get(code);

      if (embeddings == null) {
        codeEmbeddingsMap.put(code, Sets.newHashSet(embedding));
      } else {
        embeddings.add(embedding);
      }
    }

    return graph;
  }
}
