package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.algorithms.fsm.tuples.AdjacencyList;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SearchSpaceItem {

  private final Map<GradoopId, AdjacencyList> adjacencyLists;
  private final Map<CompressedDfsCode, Collection<DfsEmbedding>> codeEmbeddingsMap;

  public SearchSpaceItem(Map<GradoopId, AdjacencyList> adjacencyLists,
    Map<CompressedDfsCode, Collection<DfsEmbedding>> codeEmbeddingsMap) {

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

    for(Map.Entry<CompressedDfsCode, Collection<DfsEmbedding>> entry :
      codeEmbeddingsMap.entrySet()) {

      s += "\n\t\t" + entry.getKey().getDfsCode();

      for(DfsEmbedding embedding : entry.getValue()) {
        s += embedding;
      }
    }

    return s;
  }

  public Set<CompressedDfsCode> getDfsCodes() {
    return codeEmbeddingsMap.keySet();
  }
}
