package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpaceItem;
import org.gradoop.model.impl.algorithms.fsm.tuples.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleVertex;
import org.gradoop.model.impl.id.GradoopId;

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

      adjacencyLists.get(sourceId).add(AdjacencyListEntry.newOutgoing(
        edgeId, edgeLabel, targetId, targetLabel));

      adjacencyLists.get(targetId).add(AdjacencyListEntry.newIncoming(
        edgeId, edgeLabel, sourceId, sourceLabel));
    }

    return new SearchSpaceItem(adjacencyLists);
  }
}
