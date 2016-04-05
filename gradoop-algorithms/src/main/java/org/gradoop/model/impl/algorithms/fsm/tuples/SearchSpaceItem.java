package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsEmbedding;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * instances either represent graphs OR the collector for frequent DFS codes
 *
 * f0 : graphId                           collectorId
 * f1 : false                             true
 * f2 :              true, if active
 * f3 : map vertexId - adjacency list     empty map
 * f4 : map DFS code - embeddings         empty map
 * f5 : empty array                       frequent DFS codes
 */
public class SearchSpaceItem extends Tuple6<
    GradoopId,
    Boolean,
    Boolean,
    HashMap<GradoopId, AdjacencyList>,
    HashMap<CompressedDfsCode, HashSet<DfsEmbedding>>,
    CompressedDfsCode[]
  > {

  public SearchSpaceItem() {
  }

  public SearchSpaceItem(
    GradoopId graphId,
    boolean collector,
    boolean active,
    HashMap<GradoopId, AdjacencyList> adjacencyLists,
    HashMap<CompressedDfsCode, HashSet<DfsEmbedding>> codeEmbeddings,
    CompressedDfsCode[] frequentDfsCodes) {

    setId(graphId);
    setCollector(collector);
    setActive(active);
    setAdjacencyLists(adjacencyLists);
    setCodeEmbeddings(codeEmbeddings);
    setFrequentDfsCodes(frequentDfsCodes);
  }

  public void setId(GradoopId id) {
    this.f0 = id;
  }

  public Boolean isCollector() {
    return this.f1;
  }

  public void setCollector(Boolean collector) {
    this.f1 = collector;
  }

  public Boolean isActive() {
    return this.f2;
  }

  public void setActive(Boolean active) {
    this.f2 = active;
  }

  public Map<GradoopId, AdjacencyList> getAdjacencyLists() {
    return f3;
  }

  public void setAdjacencyLists(
    HashMap<GradoopId, AdjacencyList> adjacencyLists) {

    this.f3 = adjacencyLists;
  }

  public HashMap<CompressedDfsCode, HashSet<DfsEmbedding>>
  getCodeEmbeddings() {
    return f4;
  }

  public void setCodeEmbeddings(
    HashMap<CompressedDfsCode, HashSet<DfsEmbedding>> codeEmbeddings) {
    this.f4 = codeEmbeddings;
  }

  public CompressedDfsCode[] getFrequentDfsCodes() {
    return this.f5;
  }

  public void setFrequentDfsCodes(CompressedDfsCode[] collectedDfsCodes) {
    this.f5 = collectedDfsCodes;
  }

  @Override
  public String toString() {

    String s = "SearchSpaceItem";


    if(isCollector()) {
      s += " (Collector)\n\tFrequent DFS codes\n";

      for(CompressedDfsCode compressedDfsCode : getFrequentDfsCodes()) {
        s+= "\n" + compressedDfsCode;
      }
    } else {
      s += " (Graph)\n\tAdjacency lists";

      for(Map.Entry<GradoopId, AdjacencyList> entry :
        getAdjacencyLists().entrySet()) {

        s += "\n\t\t(" + entry.getValue().getVertexLabel() + ":" +
          entry.getKey() + ") : " +
          StringUtils.join(entry.getValue().getEntries(), " | ");
      }

      s += "\n\tDFS codes and embeddings";

      for(Map.Entry<CompressedDfsCode, HashSet<DfsEmbedding>> entry :
        getCodeEmbeddings().entrySet()) {

        s += "\n\t\t" + entry.getKey().getDfsCode();

        for(DfsEmbedding embedding : entry.getValue()) {
          s += embedding;
        }
      }
    }

    return s;
  }

  public static SearchSpaceItem createForGraph(GradoopId graphId,
    HashMap<GradoopId, AdjacencyList> adjacencyLists,
    HashMap<CompressedDfsCode, HashSet<DfsEmbedding>> codeEmbeddings) {

    return new SearchSpaceItem(
      graphId,
      false,
      true,
      adjacencyLists,
      codeEmbeddings,
      new CompressedDfsCode[0]
    );
  }


  public static SearchSpaceItem createCollector() {

    HashMap<GradoopId, AdjacencyList> adjacencyLists = new HashMap<>();
    HashMap<CompressedDfsCode, HashSet<DfsEmbedding>> codeEmbeddings = new
      HashMap<>();

    return new SearchSpaceItem(
      GradoopId.NULL_VALUE,
      true,
      true,
      adjacencyLists,
      codeEmbeddings,
      new CompressedDfsCode[0]
    );
  }
}
