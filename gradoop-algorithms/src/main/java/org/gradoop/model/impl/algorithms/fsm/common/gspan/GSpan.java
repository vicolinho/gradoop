package org.gradoop.model.impl.algorithms.fsm.common.gspan;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanEdge;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;


import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public static GSpanTransaction createTransaction(CompressedDfsCode dfsCode) {

    // turn DFS edges into gSpan edges
    List<DFSStep> steps = dfsCode.getDfsCode().getSteps();
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

    Map<CompressedDfsCode, Collection<DFSEmbedding>> codeEmbeddings =
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
        sourceId, sourceLabel, true, edgeId, edgeLabel, targetId, targetLabel);

      addEntry(adjacencyLists,
        targetId, targetLabel, false, edgeId, edgeLabel, sourceId, sourceLabel);
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

  private static void createAdjacencyListsAndEdges(final List<DFSStep> steps,
    final List<AdjacencyList> adjacencyLists, final List<GSpanEdge> edges) {

    int edgeId = 0;
    for(DFSStep step : steps) {

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
    Map<CompressedDfsCode, Collection<DFSEmbedding>> codeEmbeddings,
    GSpanEdge edge) {

    CompressedDfsCode code = initDfsCode(edge);
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

  private static CompressedDfsCode initDfsCode(final GSpanEdge edge) {

    int sourceLabel = edge.getSourceLabel();
    int edgeLabel = edge.getLabel();
    int targetLabel = edge.getTargetLabel();

    DFSStep step;

    if (edge.isLoop()) {
      step = new DFSStep(0, sourceLabel, true, edgeLabel, 0, sourceLabel);
    } else if(edge.sourceIsMinimumLabel()) {
      step = new DFSStep(0, sourceLabel, true, edgeLabel, 1, targetLabel);
    } else {
      step = new DFSStep(0, targetLabel, false, edgeLabel, 1, sourceLabel);
    }

    return new CompressedDfsCode(new DfsCode(step));
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

  public static void growFrequentSubgraphs(final GSpanTransaction transaction,
    Collection<CompressedDfsCode> frequentParentSubgraphs, FSMConfig fsmConfig)
  {
    Map<CompressedDfsCode, Collection<DFSEmbedding>> childCodeEmbeddings = null;

    for(CompressedDfsCode frequentSubgraph : frequentParentSubgraphs) {
      Collection<DFSEmbedding> parentEmbeddings =
        transaction.getCodeEmbeddings().get(frequentSubgraph);

      if (parentEmbeddings != null) {
        DfsCode parentCode = frequentSubgraph.getDfsCode();
        int minVertexLabel = frequentSubgraph.getMinVertexLabel();

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

                    childCode.getSteps().add(new DFSStep(
                      fromVertexTime,
                      fromVertexLabel,
                      entry.isOutgoing(),
                      entry.getEdgeLabel(),
                      toVertexTime,
                      toVertexLabel
                    ));

                    childEmbedding.getEdgeTimes().add(edgeId);

                    CompressedDfsCode compressedChildCode =
                      new CompressedDfsCode(childCode);

                    childCodeEmbeddings = addCodeEmbedding(
                      childCodeEmbeddings, compressedChildCode, childEmbedding);
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

  public static CompressedDfsCode minimumDfsCode(
    final Collection<CompressedDfsCode> dfsCodes, final FSMConfig fsmConfig) {

    Iterator<CompressedDfsCode> iterator = dfsCodes.iterator();

    DfsCode minCode = iterator.next().getDfsCode();

    for(CompressedDfsCode subgraph : dfsCodes) {
      DfsCode nextCode = subgraph.getDfsCode();

      if(getSiblingComparator(fsmConfig).compare(nextCode, minCode) < 0) {
        minCode = nextCode;
      }
    }

    CompressedDfsCode minSubgraph = new CompressedDfsCode(minCode);
    minSubgraph.setMinVertexLabel(minCode.getMinVertexLabel());

    return minSubgraph;
  }

  private static DfsCodeSiblingComparator getSiblingComparator(
    FSMConfig fsmConfig) {
    return new DfsCodeSiblingComparator(fsmConfig.isDirected());
  }

  private static Map<CompressedDfsCode, Collection<DFSEmbedding>>
  addCodeEmbedding(
    Map<CompressedDfsCode, Collection<DFSEmbedding>> codeEmbeddings,
    CompressedDfsCode code, DFSEmbedding embedding) {

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
    CompressedDfsCode subgraph, FSMConfig fsmConfig) {

    GSpanTransaction transaction = createTransaction(subgraph);
    CompressedDfsCode minDfsCode = getMinimumDFSCode(transaction, fsmConfig);

    return subgraph.equals(minDfsCode);
  }

  public static CompressedDfsCode getMinimumDFSCode(
    GSpanTransaction transaction, FSMConfig fsmConfig) {
    CompressedDfsCode minDfsCode = null;

    while (transaction.hasGrownSubgraphs()) {
      Set<CompressedDfsCode> grownSubgraphs =
        transaction.getCodeEmbeddings().keySet();

      minDfsCode = minimumDfsCode(grownSubgraphs, fsmConfig);


      growFrequentSubgraphs(
        transaction, Lists.newArrayList(minDfsCode), fsmConfig);

    }
    return minDfsCode;
  }
}
