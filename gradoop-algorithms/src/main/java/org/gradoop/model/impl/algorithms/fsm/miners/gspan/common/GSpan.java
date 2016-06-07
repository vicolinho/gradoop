package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.comparators.DfsCodeSiblingComparator;

import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsStep;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .GSpanEdge;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.id.GradoopId;

import java.util.*;

public class GSpan {

  /**
   * creates the gSpan mining representation of a graph transaction
   *
   * @param triples the graphs edges
   * @return graph transaction
   */
  public static GSpanTransaction createTransaction(
    Iterable<EdgeTriple> triples) {

    // replace GradoopIds by Integer Ids
    List<GSpanEdge> edges = Lists.newArrayList();
    List<AdjacencyList> adjacencyLists = Lists.newArrayList();
    createAdjacencyListsAndEdges(triples, adjacencyLists, edges);

    return createTransaction(adjacencyLists, edges);
  }

  public static GSpanTransaction createTransaction(DfsCode dfsCode) {

    // turn DFS edges into gSpan edges
    List<DfsStep> steps = dfsCode.getSteps();
    List<AdjacencyList> adjacencyLists = Lists.newArrayList();
    List<GSpanEdge> edges = Lists.newArrayListWithExpectedSize(steps.size());
    createAdjacencyListsAndEdges(steps, adjacencyLists, edges);

    return createTransaction(adjacencyLists, edges);
  }

  private static GSpanTransaction createTransaction(
    List<AdjacencyList> adjacencyLists, List<GSpanEdge> edges) {
    // sort by min vertex label
    Collections.sort(edges);

    // create adjacency lists and 1-edge DFS codes with their embeddings

    Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();

    Iterator<GSpanEdge> iterator = edges.iterator();
    GSpanEdge lastEdge = iterator.next();

    Collection<DFSEmbedding> embeddings =
      createNewDfsCodeEmbeddings(codeEmbeddings, lastEdge);

    while (iterator.hasNext()){
      GSpanEdge edge = iterator.next();

      // add embedding of 1-edge code
      if (edge.compareTo(lastEdge) == 0) {
        embeddings.add(initDfsEmbedding(edge));
      } else {
        embeddings = createNewDfsCodeEmbeddings(codeEmbeddings, edge);
        lastEdge = edge;
      }
    }

    return new GSpanTransaction(adjacencyLists, codeEmbeddings);
  }

  /**
   * turns edge triples into gSpan edges,
   * i.e., replaces GradoopIds by local integer ids
   *
   * @param iterable edge triples with GradoopIds
   * @param adjacencyLists
   *@param edges  @return gSpan edges
   */
  private static void createAdjacencyListsAndEdges(
    final Iterable<EdgeTriple> iterable,
    final List<AdjacencyList> adjacencyLists, final List<GSpanEdge> edges) {

    Map<GradoopId, Integer> vertexIdMap = new HashMap<>();
    int vertexId = 0;

    int edgeId = 0;
    for(EdgeTriple triple : iterable) {

      Integer edgeLabel = triple.getEdgeLabel();

      GradoopId sourceGradoopId = triple.getSourceId();
      Integer sourceId = vertexIdMap.get(sourceGradoopId);
      Integer sourceLabel = triple.getSourceLabel();

      if(sourceId == null) {
        sourceId = vertexId;
        vertexIdMap.put(sourceGradoopId, sourceId);
        vertexId++;
      }

      GradoopId targetGradoopId = triple.getTargetId();
      Integer targetId = vertexIdMap.get(targetGradoopId);
      Integer targetLabel = triple.getTargetLabel();

      if(targetId == null) {
        targetId = vertexId;
        vertexIdMap.put(targetGradoopId, targetId);

        vertexId++;
      }

      addEntries(edges, adjacencyLists,
        sourceId, sourceLabel, edgeId, edgeLabel, targetId, targetLabel);

      edgeId++;
    }
  }

  private static void addEntries(
    final List<GSpanEdge> edges,
    final List<AdjacencyList> adjacencyLists,
    int sourceId, int sourceLabel,
    int edgeId, int edgeLabel,
    int targetId, int targetLabel) {

    edges.add(new GSpanEdge(
      sourceId, sourceLabel, edgeId, edgeLabel, targetId, targetLabel));

    if(sourceId <= targetId) {
      addEntry(adjacencyLists,
        sourceId, sourceLabel, true, edgeId, edgeLabel, targetId, targetLabel);

      addEntry(adjacencyLists,
        targetId, targetLabel, false, edgeId, edgeLabel, sourceId, sourceLabel);
    } else {
      addEntry(adjacencyLists,
        targetId, targetLabel, false, edgeId, edgeLabel, sourceId, sourceLabel);

      addEntry(adjacencyLists,
        sourceId, sourceLabel, true, edgeId, edgeLabel, targetId, targetLabel);
    }
  }

  private static void addEntry(final List<AdjacencyList> adjacencyLists,
    int fromId, int fromLabel,
    boolean outgoing, int edgeId, int edgeLabel,
    int toId, int toLabel) {

    AdjacencyList adjacencyList;

    if (fromId >= adjacencyLists.size() ) {
      adjacencyList = new AdjacencyList(fromLabel);
      adjacencyLists.add(adjacencyList);
    } else {
      adjacencyList = adjacencyLists.get(fromId);
    }

    adjacencyList.getEntries().add(new AdjacencyListEntry(
      outgoing, edgeId, edgeLabel, toId, toLabel));
  }

  private static void createAdjacencyListsAndEdges(final List<DfsStep> steps,
    final List<AdjacencyList> adjacencyLists, final List<GSpanEdge> edges) {

    int edgeId = 0;
    for(DfsStep step : steps) {

      Integer edgeLabel = step.getEdgeLabel();

      Integer sourceId;
      Integer sourceLabel;

      Integer targetId;
      Integer targetLabel;

      if (step.isOutgoing()) {
        sourceId = step.getFromTime();
        sourceLabel = step.getFromLabel();
        targetId = step.getToTime();
        targetLabel = step.getToLabel();
      } else {
        sourceId = step.getToTime();
        sourceLabel = step.getToLabel();
        targetId = step.getFromTime();
        targetLabel = step.getFromLabel();
      }

      addEntries(edges, adjacencyLists,
        sourceId, sourceLabel, edgeId, edgeLabel, targetId, targetLabel);

      edgeId++;
    }

  }


  private static Collection<DFSEmbedding> createNewDfsCodeEmbeddings(
    Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings,
    GSpanEdge edge) {

    DfsCode code = initDfsCode(edge);
    DFSEmbedding embedding = initDfsEmbedding(edge);
    Collection<DFSEmbedding> embeddings = Lists.newArrayList(embedding);
    codeEmbeddings.put(code, embeddings);

    return embeddings;
  }

  private static DFSEmbedding initDfsEmbedding(final GSpanEdge edge) {
    List<Integer> vertexTimes;

    if (edge.isLoop()) {
      vertexTimes = Lists.newArrayList(edge.getSourceId());
    } else if (edge.getSourceLabel() <= edge.getTargetLabel()) {
      vertexTimes = Lists.newArrayList(edge.getSourceId(), edge.getTargetId());
    } else {
      vertexTimes = Lists.newArrayList(edge.getTargetId(), edge.getSourceId());
    }

    return new DFSEmbedding(vertexTimes, Lists.newArrayList(edge.getEdgeId()));
  }

  private static DfsCode initDfsCode(final GSpanEdge edge) {

    int sourceLabel = edge.getSourceLabel();
    int edgeLabel = edge.getLabel();
    int targetLabel = edge.getTargetLabel();

    DfsStep step;

    if (edge.isLoop()) {
      step = new DfsStep(0, sourceLabel, true, edgeLabel, 0, sourceLabel);
    } else if(edge.sourceIsMinimumLabel()) {
      step = new DfsStep(0, sourceLabel, true, edgeLabel, 1, targetLabel);
    } else {
      step = new DfsStep(0, targetLabel, false, edgeLabel, 1, sourceLabel);
    }

    return new DfsCode(step);
  }



  private static void createAdjacencyListEntries(
    List<AdjacencyList> adjacencyLists, GSpanEdge edge) {

    System.out.println(adjacencyLists);

    int sourceId = edge.getSourceId();
    int sourceLabel = edge.getSourceLabel();

    int edgeId = edge.getEdgeId();
    int edgeLabel = edge.getLabel();

    int targetId = edge.getTargetId();
    int targetLabel = edge.getTargetLabel();

    AdjacencyList sourceAdjacencyList;

    sourceAdjacencyList =
      getOrCreateAdjacencyList(adjacencyLists, sourceId, sourceLabel);

    AdjacencyList targetAdjacencyList;

    if(edge.isLoop()) {
      targetAdjacencyList = sourceAdjacencyList;
    } else {
      targetAdjacencyList =
        getOrCreateAdjacencyList(adjacencyLists, targetId, targetLabel);
    }

    sourceAdjacencyList.getEntries().add(
      new AdjacencyListEntry(true, edgeId, edgeLabel, targetId, targetLabel));

    targetAdjacencyList.getEntries().add(
      new AdjacencyListEntry(false, edgeId, edgeLabel, sourceId, sourceLabel));

    System.out.println(adjacencyLists);

  }

  private static AdjacencyList getOrCreateAdjacencyList(
    final List<AdjacencyList> adjacencyLists, final int id, final int label) {

    AdjacencyList adjacencyList = adjacencyLists.get(id);

    if (adjacencyList == null) {
      adjacencyList = new AdjacencyList(label);
      adjacencyLists.add(id, adjacencyList);
    }

    return adjacencyList;
  }

  public static void growEmbeddings(final GSpanTransaction transaction,
    Collection<DfsCode> frequentParentSubgraphs, FsmConfig fsmConfig)
  {
    Map<DfsCode, Collection<DFSEmbedding>> childCodeEmbeddings = null;

    for(DfsCode parentCode : frequentParentSubgraphs) {
      Collection<DFSEmbedding> parentEmbeddings =
        transaction.getCodeEmbeddings().get(parentCode);

      if (parentEmbeddings != null) {
        int minVertexLabel = parentCode.getMinVertexLabel();

        List<Integer> rightmostPath = parentCode.getRightMostPathVertexTimes();

        // for each embedding
        for (DFSEmbedding parentEmbedding : parentEmbeddings) {

          // first iterated vertex is rightmost
          Boolean rightMostVertex = true;

          Set<Integer> visitedEdges =
            Sets.newHashSet(parentEmbedding.getEdgeTimes());

          Map<Integer, Integer> vertexIdTime = mapVertexTimes(parentEmbedding);

          // for each time on rightmost path
          for (Integer fromVertexTime : rightmostPath) {
            Integer fromVertexId =
              parentEmbedding.getVertexTimes().get(fromVertexTime);

            // query fromVertex data
            AdjacencyList adjacencyList =
              transaction.getAdjacencyLists().get(fromVertexId);

            Integer fromVertexLabel = adjacencyList.getFromVertexLabel();

            // for each incident edge
            for (AdjacencyListEntry entry : adjacencyList.getEntries()) {

              // if valid extension for branch
              int toVertexLabel = entry.getToVertexLabel();

              if (toVertexLabel >= minVertexLabel) {

                Integer edgeId = entry.getEdgeId();

                // if edge not already contained
                if (!visitedEdges.contains(edgeId)) {

                  // query toVertexData
                  Integer toVertexId = entry.getToVertexId();
                  Integer toVertexTime = vertexIdTime.get(toVertexId);

                  boolean forward = toVertexTime == null;

                  // PRUNING : grow only forward
                  // or backward from rightmost vertex
                  if (forward || rightMostVertex) {

                    DFSEmbedding childEmbedding =
                      DFSEmbedding.deepCopy(parentEmbedding);
                    DfsCode childCode = DfsCode.deepCopy(parentCode);

                    // add new vertex to embedding for forward steps
                    if (forward) {
                      childEmbedding.getVertexTimes().add(toVertexId);
                      toVertexTime = vertexIdTime.size();
                    }

                    childCode.getSteps().add(new DfsStep(
                      fromVertexTime,
                      fromVertexLabel,
                      entry.isOutgoing(),
                      entry.getEdgeLabel(),
                      toVertexTime,
                      toVertexLabel
                    ));

                    childEmbedding.getEdgeTimes().add(edgeId);;

                    childCodeEmbeddings = addCodeEmbedding(
                      childCodeEmbeddings, childCode, childEmbedding);
                  }
                }
              }
            }
            rightMostVertex = false;
          }
        }

      }
    }

    transaction.setCodeEmbeddings(childCodeEmbeddings);
  }

  private static Map<Integer, Integer> mapVertexTimes(DFSEmbedding embedding) {
    Map<Integer, Integer> vertexTimes = Maps.newHashMapWithExpectedSize(
        embedding.getVertexTimes().size());

    int vertexTime = 0;
    for(int vertexId : embedding.getVertexTimes()) {
      vertexTimes.put(vertexId, vertexTime);
      vertexTime++;
    }
    return vertexTimes;
  }

  public static DfsCode minimumDfsCode(
    final Collection<DfsCode> dfsCodes, final FsmConfig fsmConfig) {

    Iterator<DfsCode> iterator = dfsCodes.iterator();

    DfsCode minCode = iterator.next();

    for(DfsCode nextCode : dfsCodes) {
      if(getSiblingComparator(fsmConfig).compare(nextCode, minCode) < 0) {
        minCode = nextCode;
      }
    }

    return minCode;
  }

  private static DfsCodeSiblingComparator getSiblingComparator(
    FsmConfig fsmConfig) {
    return new DfsCodeSiblingComparator(fsmConfig.isDirected());
  }

  private static Map<DfsCode, Collection<DFSEmbedding>>
  addCodeEmbedding(
    Map<DfsCode, Collection<DFSEmbedding>> codeEmbeddings,
    DfsCode code, DFSEmbedding embedding) {

    Collection<DFSEmbedding> embeddings;

    if(codeEmbeddings == null) {
      codeEmbeddings = Maps.newHashMap();
      codeEmbeddings.put(code, Lists.newArrayList(embedding));
    } else {
      embeddings = codeEmbeddings.get(code);

      if(embeddings == null) {
        codeEmbeddings.put(code, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }
    return codeEmbeddings;
  }

  public static boolean isMinimumDfsCode(
    DfsCode subgraph, FsmConfig fsmConfig) {

    GSpanTransaction transaction = createTransaction(subgraph);
    DfsCode minDfsCode = getMinimumDFSCode(transaction, fsmConfig);

    return subgraph.equals(minDfsCode);
  }

  public static DfsCode getMinimumDFSCode(
    GSpanTransaction transaction, FsmConfig fsmConfig) {
    DfsCode minDfsCode = null;

    while (transaction.hasGrownSubgraphs()) {
      Set<DfsCode> grownSubgraphs =
        transaction.getCodeEmbeddings().keySet();

      minDfsCode = minimumDfsCode(grownSubgraphs, fsmConfig);


      growEmbeddings(
        transaction, Lists.newArrayList(minDfsCode), fsmConfig);

    }
    return minDfsCode;
  }

  public static boolean contains(
    GSpanTransaction graph, DfsCode subgraph, FsmConfig fsmConfig) {


    Iterator<DfsStep> iterator = subgraph.getSteps().iterator();

    DfsStep step = iterator.next();

    Collection<DFSEmbedding> parentEmbeddings = Lists.newArrayList();
    int fromVertexId = 0;
    for(AdjacencyList adjacencyList : graph.getAdjacencyLists()) {
      if (step.getFromLabel().equals(adjacencyList.getFromVertexLabel())) {
        for (AdjacencyListEntry entry : adjacencyList.getEntries()) {
          if (step.isOutgoing() == entry.isOutgoing() &&
            step.getEdgeLabel().equals(entry.getEdgeLabel()) &&
            step.getToLabel().equals(entry.getToVertexLabel())) {

            List<Integer> vertexTimes = step.isLoop() ?
              Lists.newArrayList(fromVertexId) :
              Lists.newArrayList(fromVertexId, entry.getToVertexId());

            List<Integer> edgesTimes = Lists.newArrayList(entry.getEdgeId());
            parentEmbeddings.add(new DFSEmbedding(vertexTimes, edgesTimes));
          }
        }
      }
      fromVertexId++;
    }

    while (iterator.hasNext() && !parentEmbeddings.isEmpty()) {
      Collection<DFSEmbedding> childEmbeddings = Lists.newArrayList();

      step = iterator.next();

      for (DFSEmbedding parentEmbedding : parentEmbeddings) {
        fromVertexId = parentEmbedding.getVertexTimes().get(step.getFromTime());

        for (AdjacencyListEntry entry :
          graph.getAdjacencyLists().get(fromVertexId).getEntries()) {

          // edge not contained
          if (!parentEmbedding.getEdgeTimes().contains(entry.getEdgeId())) {
            // forward traversal
            if (step.isForward() &&
              // same to vertex label
              step.getToLabel().equals(entry.getToVertexLabel()) &&
              // same edge label
              step.getEdgeLabel().equals(entry.getEdgeLabel()) &&
              // vertex not contained
              ! parentEmbedding.getVertexTimes()
                .contains(entry.getToVertexId())) {

              DFSEmbedding childEmbedding =
                DFSEmbedding.deepCopy(parentEmbedding);
              childEmbedding.getVertexTimes().add(entry.getToVertexId());
              childEmbedding.getEdgeTimes().add(entry.getEdgeId());

              childEmbeddings.add(childEmbedding);

              // backward traversal
            } else if (step.isBackward() &&
              // same edge label
              step.getEdgeLabel().equals(entry.getEdgeLabel()) &&
              // vertex already mapped to correct time
              parentEmbedding.getVertexTimes().get(step.getToTime())
                .equals(entry.getToVertexId())) {

              DFSEmbedding childEmbedding =
                DFSEmbedding.deepCopy(parentEmbedding);
              childEmbedding.getEdgeTimes().add(entry.getEdgeId());

              childEmbeddings.add(childEmbedding);
            }
          }
        }
      }

      parentEmbeddings = childEmbeddings;
    }

    return !parentEmbeddings.isEmpty();
  }
}
