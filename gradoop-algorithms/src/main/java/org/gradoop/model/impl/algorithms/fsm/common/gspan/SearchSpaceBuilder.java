package org.gradoop.model.impl.algorithms.fsm.common.gspan;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.SlimEdge;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Transaction;
import org.gradoop.model.impl.id.GradoopId;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchSpaceBuilder implements Serializable {

  private final DfsCodeComparator dfsCodeComparator;

  public SearchSpaceBuilder(FSMConfig fsmConfig) {
    dfsCodeComparator = new DfsCodeComparator(fsmConfig.isDirected());
  }

  public Transaction createTransaction(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable) {

    Map<DFSCode, Collection<SlimEdge>> codeEdges = createCodeEdges(iterable);

    return createTransaction(codeEdges);
  }

  private Map<DFSCode, Collection<SlimEdge>> createCodeEdges(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable) {
    Map<DFSCode, Collection<SlimEdge>> codeEdges = Maps.newHashMap();

    Map<GradoopId, Integer> vertexMap = new HashMap<>();
    int vertexId = 0;
    int edgeId = 0;

    for(Tuple3<GradoopId, FatEdge, CompressedDFSCode> triple : iterable) {

      FatEdge fatEdge = triple.f1;
      DFSCode code = triple.f2.getDfsCode();

      GradoopId minGradoopId = fatEdge.getMinId();
      Integer minId = vertexMap.get(minGradoopId);
      if(minId == null) {
        minId = vertexId;
        vertexMap.put(minGradoopId, minId);
        vertexId++;
      }

      GradoopId maxGradoopId = fatEdge.getMaxId();
      Integer maxId = vertexMap.get(maxGradoopId);
      if(maxId == null) {
        maxId = vertexId;
        vertexMap.put(maxGradoopId, maxId);
        vertexId++;
      }

      SlimEdge slimEdge = new SlimEdge(
        minId, fatEdge.getMaxLabel(),
        edgeId, fatEdge.isOutgoing(), fatEdge.getLabel(),
        maxId, fatEdge.getMaxLabel()
      );

      Collection<SlimEdge> siblings = codeEdges.get(code);

      if (siblings == null) {
        codeEdges.put(code, Lists.newArrayList(slimEdge));
        } else {
        siblings.add(slimEdge);
        }

      edgeId++;
    }
    return codeEdges;
  }

  private Transaction createTransaction(
    Map<DFSCode, Collection<SlimEdge>> codeEdges) {

    Map<Integer, AdjacencyList> adjacencyLists = Maps.newHashMap();
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();
    Collection<Collection<DFSCode>> codeSiblings = Lists.newArrayList();

    List<DFSCode> oneEdgeCodes = Lists.newArrayList(codeEdges.keySet());

    Collections.sort(oneEdgeCodes, dfsCodeComparator);

    int minEdgePatternId = 0;

    for(DFSCode code : oneEdgeCodes) {
      Collection<DFSEmbedding> embeddings = Lists.newArrayList();

      for(SlimEdge edge : codeEdges.get(code)) {

        updateAdjacencyLists(adjacencyLists, edge, minEdgePatternId);

        List<Integer> vertexTimes = edge.isLoop() ?
          Lists.newArrayList(edge.getMinId()) :
          Lists.newArrayList(edge.getMinId(), edge.getMaxId());

        List<Integer> edgeTimes = Lists.newArrayList(edge.getEdgeId());

        embeddings.add(
          new DFSEmbedding(minEdgePatternId, vertexTimes, edgeTimes));
      }

      codeEmbeddings.put(new CompressedDFSCode(code), embeddings);

      minEdgePatternId++;
    }

    return new Transaction(adjacencyLists, codeEmbeddings, codeSiblings);
  }

  private void updateAdjacencyLists(Map<Integer, AdjacencyList> adjacencyLists,
    SlimEdge edge, int minEdgePatternId) {

    int minId = edge.getMinId();
    int minLabel = edge.getMinLabel();

    int edgeId = edge.getEdgeId();
    boolean outgoing = edge.isOutgoing();
    boolean loop = edge.isLoop();
    int edgeLabel = edge.getLabel();

    int maxId = edge.getMaxId();
    int maxLabel = edge.getMaxLabel();


    AdjacencyList minAdjacencyList = adjacencyLists.get(minId);

    if(minAdjacencyList == null) {
      minAdjacencyList = new AdjacencyList(minLabel);
      adjacencyLists.put(minId, minAdjacencyList);
    }

    AdjacencyList maxAdjacencyList;

    if(loop) {
      maxAdjacencyList = minAdjacencyList;
    } else {
      maxAdjacencyList = adjacencyLists.get(maxId);

      if(maxAdjacencyList == null) {
        maxAdjacencyList = new AdjacencyList(maxLabel);
        adjacencyLists.put(maxId, maxAdjacencyList);
      }
    }

    minAdjacencyList.getEntries().add(new AdjacencyListEntry(
      minEdgePatternId, outgoing, edgeId, edgeLabel, maxId, maxLabel));

    maxAdjacencyList.getEntries().add(new AdjacencyListEntry(
      minEdgePatternId, !outgoing, edgeId, edgeLabel, minId, minLabel));
  }
}
