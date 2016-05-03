package org.gradoop.model.impl.algorithms.fsm.common.gspan;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class SearchSpaceBuilder implements Serializable {

  public void initAdjacencyListsAndCodeEmbeddings(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    ArrayList<AdjacencyList> adjacencyLists,
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddingsMap) {
    HashMap<GradoopId, Integer> vertexIndexMap = new HashMap<>();

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
  }
}
