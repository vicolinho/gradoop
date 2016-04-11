package org.gradoop.model.impl.operators.tostring;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphTransaction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.model.impl.operators.tostring.pojos.DFSCode;
import org.gradoop.model.impl.operators.tostring.pojos.DFSEmbedding;
import org.gradoop.model.impl.operators.tostring.pojos.DFSStep;

import java.util.Collection;
import java.util.Map;

public class MinDFSCode
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<GraphTransaction<G, V, E>, String> {

  private final GraphHeadToString<G> graphHeadToString;
  private final VertexToString<V> vertexToString;
  private final EdgeToString<E> edgeToString;

  public MinDFSCode(
    GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString,
    EdgeToString<E> edgeToString) {

    this.graphHeadToString = graphHeadToString;
    this.vertexToString = vertexToString;
    this.edgeToString = edgeToString;

  }

  @Override
  public String map(GraphTransaction<G, V, E> transaction) throws
    Exception {

    Map<GradoopId, String> vertexLabels = Maps
      .newHashMapWithExpectedSize(transaction.getVertices().size());

    for(V vertex : transaction.getVertices()) {
      vertexLabels.put(vertex.getId(), vertexToString.label(vertex));
    }

    Map<DFSCode, Collection<DFSEmbedding>> codeEmbeddings = Maps.newHashMap();

    for(E edge : transaction.getEdges()) {
      GradoopId sourceId = edge.getSourceId();
      GradoopId targetId = edge.getTargetId();

      String sourceLabel = vertexLabels.get(sourceId);
      String edgeLabel = edgeToString.label(edge);
      String targetLabel = vertexLabels.get(targetId);

      boolean loop = sourceId.equals(targetId);
      boolean outgoing = sourceLabel.compareTo(targetLabel) <= 0;

      DFSStep step;
      DFSEmbedding embedding;

      if(loop) {
        embedding = new DFSEmbedding(
          Lists.newArrayList(sourceId), edge.getId());
        step = new DFSStep(0, sourceLabel, true, edgeLabel, 0, sourceLabel);
      } else if(outgoing) {
        embedding = new DFSEmbedding(
          Lists.newArrayList(sourceId, targetId), edge.getId());
        step = new DFSStep(0, sourceLabel, true, edgeLabel, 1, targetLabel);
      } else {
        embedding = new DFSEmbedding(
          Lists.newArrayList(targetId, sourceId), edge.getId());
        step = new DFSStep(0, targetLabel, false, edgeLabel, 1, sourceLabel);
      }

      DFSCode code = new DFSCode(step);
      Collection<DFSEmbedding> embeddings = codeEmbeddings.get(code);

      if(embeddings == null) {
        codeEmbeddings.put(code, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }

    return StringUtils.join(codeEmbeddings.keySet(), "\n");
  }
}
