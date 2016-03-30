package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsEmbedding;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsStep;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpaceItem;
import org.gradoop.model.impl.algorithms.fsm.tuples.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SearchSpace
  implements JoinFunction<Tuple2<GradoopId,Collection<SimpleVertex>>,
    Tuple2<GradoopId, Collection<SimpleEdge>>, SearchSpaceItem> {

  @Override
  public SearchSpaceItem join(
    Tuple2<GradoopId, Collection<SimpleVertex>> graphVertices,
    Tuple2<GradoopId, Collection<SimpleEdge>> graphEdges) throws
    Exception {

    Map<GradoopId, String> vertexLabels = new HashMap<>();
    Map<GradoopId, AdjacencyList> adjacencyLists = new HashMap<>();
    Map<CompressedDfsCode, Collection<DfsEmbedding>> codeEmbeddingsMap = new HashMap<>();
    SearchSpaceItem item =
      new SearchSpaceItem(adjacencyLists, codeEmbeddingsMap);

    for(SimpleVertex vertex : graphVertices.f1) {
      vertexLabels.put(vertex.getId(), vertex.getLabel());
      adjacencyLists.put(vertex.getId(), new AdjacencyList(vertex.getLabel()));
    }

    for(SimpleEdge edge : graphEdges.f1) {

      GradoopId edgeId = edge.getId();
      String edgeLabel = edge.getLabel();

      GradoopId sourceId = edge.getSourceId();
      String sourceLabel = vertexLabels.get(sourceId);

      GradoopId targetId = edge.getTargetId();
      String targetLabel = vertexLabels.get(targetId);

      // update adjacency lists

      adjacencyLists.get(sourceId).add(AdjacencyListEntry.newOutgoing(
        edgeId, edgeLabel, targetId, targetLabel));

      if(!sourceId.equals(targetId)) {
        adjacencyLists.get(targetId).add(AdjacencyListEntry.newIncoming(
          edgeId, edgeLabel, sourceId, sourceLabel));
      }

      // update code embeddings

      ArrayList<GradoopId> vertexTimes;

      Integer fromTime = 0;
      String fromLabel;
      Boolean outgoing;
      Integer toTime;
      String toLabel;

      // loop
      if(sourceId.equals(targetId)) {
        toTime = 0;

        vertexTimes = Lists.newArrayList(sourceId);

        fromLabel = sourceLabel;
        outgoing = true;
        toLabel = targetLabel;

      } else {
        toTime = 1;

        // in direction
        if(sourceLabel.compareTo(targetLabel) < 0) {
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

      DfsStep step = new DfsStep(
        fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

      CompressedDfsCode code = new CompressedDfsCode(new DfsCode(step));
      DfsEmbedding embedding = new DfsEmbedding(vertexTimes, edgeId);

      Collection<DfsEmbedding> embeddings = codeEmbeddingsMap.get(code);

      if(embeddings == null) {
        codeEmbeddingsMap.put(code, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }

    return item;
  }
}
