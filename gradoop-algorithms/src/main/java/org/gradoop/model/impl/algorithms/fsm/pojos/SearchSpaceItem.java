package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.algorithms.fsm.tuples.AdjacencyList;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;
import java.util.Map;

/**
 * Created by peet on 30.03.16.
 */
public class SearchSpaceItem {

  private final Map<GradoopId, AdjacencyList> adjacencyLists;
  private final Map<DfsCode, Collection<DfsEmbedding>> codeEmbeddingsMap;

  public SearchSpaceItem(Map<GradoopId, AdjacencyList> adjacencyLists,
    Map<DfsCode, Collection<DfsEmbedding>> codeEmbeddingsMap) {

    this.adjacencyLists = adjacencyLists;
    this.codeEmbeddingsMap = codeEmbeddingsMap;
  }

  @Override
  public String toString() {

    String s = "SearchSpaceItem\n\tAdjacency lists";

    for(Map.Entry<GradoopId, AdjacencyList> entry : adjacencyLists.entrySet()) {
      s += "\n\t\t(" + entry.getValue().getVertexLabel() + ":" +
         entry.getKey() + ") : " +
        StringUtils.join(entry.getValue().getEntries(), " | ");
    }

    s += "\n\tDFS codes and embeddings";

    for(Map.Entry<DfsCode, Collection<DfsEmbedding>> entry :
      codeEmbeddingsMap.entrySet()) {

      s += "\n\t\t" + entry.getKey();

      for(DfsEmbedding embedding : entry.getValue()) {
        s += embedding;
      }
    }

    return s;
  }
}
