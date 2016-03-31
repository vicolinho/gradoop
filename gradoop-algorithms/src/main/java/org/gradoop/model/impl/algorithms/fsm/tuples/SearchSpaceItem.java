package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsEmbedding;
import org.gradoop.model.impl.algorithms.fsm.tuples.AdjacencyList;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SearchSpaceItem extends Tuple6<Boolean, Map<GradoopId,
  AdjacencyList>, Map<CompressedDfsCode, Collection<DfsEmbedding>>, Boolean,
  CompressedDfsCode[], Integer> {

  public SearchSpaceItem(GradoopId graphId, Map<GradoopId, AdjacencyList> vertexAdjacencyListMap,
    Map<CompressedDfsCode, Collection<DfsEmbedding>> codeEmbeddingsMap) {

//    this.f0 = graphId;
    this.f0 = true;
    setAdjacencyLists(vertexAdjacencyListMap);
    setCodeEmbeddingsMap(codeEmbeddingsMap);
    setCollector(false);
    setCollectedDfsCodes(new CompressedDfsCode[0]);
    this.f5 = 1;
  }

  public SearchSpaceItem() {
//    this.f0 = GradoopId.get();
    this.f0 = true;

    setAdjacencyLists(new HashMap<GradoopId, AdjacencyList>());
    setCodeEmbeddingsMap(new HashMap<CompressedDfsCode,
      Collection<DfsEmbedding>>());
    setCollector(true);
    setCollectedDfsCodes(new CompressedDfsCode[0]);
    this.f5 = 1;
  }

  @Override
  public String toString() {

    String s = "SearchSpaceItem\n\tAdjacency lists";

    for(Map.Entry<GradoopId, AdjacencyList> entry : 
      getAdjacencyLists().entrySet()) {
      
      s += "\n\t\t(" + entry.getValue().getVertexLabel() + ":" +
         entry.getKey() + ") : " +
        StringUtils.join(entry.getValue().getEntries(), " | ");
    }

    s += "\n\tDFS codes and embeddings";

    for(Map.Entry<CompressedDfsCode, Collection<DfsEmbedding>> entry :
      getCodeEmbeddingsMap().entrySet()) {

      s += "\n\t\t" + entry.getKey().getDfsCode();

      for(DfsEmbedding embedding : entry.getValue()) {
        s += embedding;
      }
    }

    return s;
  }

  public Set<CompressedDfsCode> getDfsCodes() {
    return getCodeEmbeddingsMap().keySet();
  }

  public Map<GradoopId, AdjacencyList> getAdjacencyLists() {
    return f1;
  }

  public void setAdjacencyLists(Map<GradoopId, AdjacencyList> f1) {
    this.f1 = f1;
  }

  public Map<CompressedDfsCode, Collection<DfsEmbedding>> 
  getCodeEmbeddingsMap() {
    return f2;
  }

  public void setCodeEmbeddingsMap(
    Map<CompressedDfsCode, Collection<DfsEmbedding>> f2) {
    this.f2 = f2;
  }

  public Boolean isCollector() {
    return this.f3;
  }

  public CompressedDfsCode[] getCollectedDfsCodes() {
    return this.f4;
  }

  public void setCollector(Boolean collector) {
    this.f3 = collector;
  }

  public void setCollectedDfsCodes(CompressedDfsCode[] collectedDfsCodes) {
    this.f4 = collectedDfsCodes;
  }

  public Boolean isActive() {
    return this.f0 == null;
  }
}
