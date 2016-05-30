package org.gradoop.model.impl.algorithms.fsm.common.gspan;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanEdge;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;
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
    List<GSpanEdge> edges = convertEdges(triples);

    return createTransaction(edges);
  }

  private static GSpanTransaction createTransaction(CompressedDFSCode dfsCode) {

    // turn DFS edges into gSpan edges
    List<GSpanEdge> edges = convertEdges(dfsCode.getDfsCode().getSteps());

    return createTransaction(edges);
  }

  private static GSpanTransaction createTransaction(List<GSpanEdge> edges) {
    // sort by min vertex label
    Collections.sort(edges);

    // create adjacency lists and 1-edge DFS codes with their embeddings

    List<AdjacencyList> adjacencyLists = Lists.newArrayList();

    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();

    Iterator<GSpanEdge> iterator = edges.iterator();
    GSpanEdge lastEdge = iterator.next();

    update(adjacencyLists, lastEdge);

    Collection<DFSEmbedding> embeddings =
      createNewDfsCodeEmbeddings(codeEmbeddings, lastEdge);

    while (iterator.hasNext()){
      GSpanEdge edge = iterator.next();

      update(adjacencyLists, edge);

      // add embedding of 1-edge code
      if (edge.equals(lastEdge)) {
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
   * @return gSpan edges
   */
  private static List<GSpanEdge> convertEdges(
    final Iterable<EdgeTriple> iterable) {

    List<GSpanEdge> edges = Lists.newArrayList();

    Map<GradoopId, Integer> vertexIdMap = new HashMap<>();
    int vertexId = 0;
    int edgeId = 0;

    for(EdgeTriple triple : iterable) {

      GradoopId sourceGradoopId = triple.getSourceId();
      Integer sourceId = vertexIdMap.get(sourceGradoopId);
      if(sourceId == null) {
        sourceId = vertexId;
        vertexIdMap.put(sourceGradoopId, sourceId);
        vertexId++;
      }

      GradoopId targetGradoopId = triple.getTargetId();
      Integer targetId = vertexIdMap.get(targetGradoopId);
      if(targetId == null) {
        targetId = vertexId;
        vertexIdMap.put(targetGradoopId, targetId);
        vertexId++;
      }

      edges.add(new GSpanEdge(
        sourceId, triple.getSourceLabel(),
        edgeId, triple.getEdgeLabel(),
        targetId, triple.getTargetLabel()
      ));

      edgeId++;
    }

    return edges;
  }

  private static List<GSpanEdge> convertEdges(List<DFSStep> steps) {

    List<GSpanEdge> edges = Lists.newArrayListWithExpectedSize(steps.size());

    int edgeId = 0;

    for(DFSStep step : steps) {
      int fromTime = step.getFromTime();
      int fromLabel = step.getFromLabel();
      int edgeLabel = step.getEdgeLabel();
      int toTime = step.getToTime();
      int toLabel = step.getToLabel();

      GSpanEdge edge;

      if (step.isLoop()) {
        edge = new GSpanEdge(
          fromTime, fromLabel, edgeId, edgeLabel, fromTime, fromLabel);
      } else if (step.isOutgoing()) {
        edge = new GSpanEdge(
          fromTime, fromLabel, edgeId, edgeLabel, toTime, toLabel);
      } else {
        edge = new GSpanEdge(
          toTime, toLabel, edgeId, edgeLabel, fromTime, fromLabel);
      }

      edges.add(edge);

      edgeId++;
    }

    return edges;
  }


  private static Collection<DFSEmbedding> createNewDfsCodeEmbeddings(
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings,
    GSpanEdge edge) {

    CompressedDFSCode code = initDfsCode(edge);
    DFSEmbedding embedding = initDfsEmbedding(edge);
    Collection<DFSEmbedding> embeddings = Lists.newArrayList(embedding);
    codeEmbeddings.put(code, embeddings);

    return embeddings;
  }

  private static DFSEmbedding initDfsEmbedding(final GSpanEdge edge) {
    List<Integer> vertexTimes;

    if (edge.isLoop()) {
      vertexTimes = Lists.newArrayList(0);
    } else {
      vertexTimes = Lists.newArrayList(0, 1);
    }

    return new DFSEmbedding(vertexTimes, Lists.newArrayList(0));
  }

  private static CompressedDFSCode initDfsCode(final GSpanEdge edge) {

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

    return new CompressedDFSCode(new DFSCode(step));
  }



  private static void update(
    List<AdjacencyList> adjacencyLists, GSpanEdge edge) {

    int sourceId = edge.getSourceId();
    int sourceLabel = edge.getSourceLabel();

    int edgeId = edge.getEdgeId();
    boolean loop = edge.isLoop();
    int edgeLabel = edge.getLabel();

    int targetId = edge.getTargetId();
    int targetLabel = edge.getTargetLabel();


    AdjacencyList sourceAdjacencyList =
      getOrCreateAdjacencyList(adjacencyLists, sourceId, sourceLabel);

    AdjacencyList targetAdjacencyList;

    if(loop) {
      targetAdjacencyList = sourceAdjacencyList;
    } else {
      targetAdjacencyList =
        getOrCreateAdjacencyList(adjacencyLists, targetId, targetLabel);
    }

    sourceAdjacencyList.getEntries().add(
      new AdjacencyListEntry(true, edgeId, edgeLabel, targetId, targetLabel));

    targetAdjacencyList.getEntries().add(
      new AdjacencyListEntry(false, edgeId, edgeLabel, sourceId, sourceLabel));
  }

  private static AdjacencyList getOrCreateAdjacencyList(
    List<AdjacencyList> adjacencyLists, int sourceId, int sourceLabel) {
    AdjacencyList sourceAdjacencyList;

    if(adjacencyLists.size() < sourceId) {
      sourceAdjacencyList = new AdjacencyList(sourceLabel);
      adjacencyLists.add(sourceId, sourceAdjacencyList);
    } else {
      sourceAdjacencyList = adjacencyLists.get(sourceId);
    }
    return sourceAdjacencyList;
  }

  public static void growFrequentSubgraphs(final GSpanTransaction transaction,
    Collection<CompressedDFSCode> frequentParentSubgraphs, FSMConfig fsmConfig)
  {
    Map<CompressedDFSCode, Collection<DFSEmbedding>> childCodeEmbeddings = null;

    for(CompressedDFSCode compressedParentCode : frequentParentSubgraphs) {
      Collection<DFSEmbedding> parentEmbeddings =
        transaction.getCodeEmbeddings().get(compressedParentCode);

      if (parentEmbeddings != null) {
        DFSCode parentCode = compressedParentCode.getDfsCode();

        int minVertexLabel = parentCode.getMinVertexLabel();

        List<Integer> rightmostPath = parentCode.getRightMostPathVertexTimes();

        // for each embedding
        for (DFSEmbedding parentEmbedding : parentEmbeddings) {

          // first iterated vertex is rightmost
          Boolean rightMostVertex = true;
          List<Integer> vertexTimes = parentEmbedding.getVertexTimes();

          // for each time on rightmost path
          for (Integer fromVertexTime : rightmostPath) {
            Integer fromVertexId = vertexTimes.get(fromVertexTime);


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
                if (!parentEmbedding.getEdgeTimes().contains(edgeId)) {

                  // query toVertexData
                  Integer toVertexId = entry.getToVertexId();
                  Integer toVertexTime = vertexTimes.indexOf(toVertexId);
                  boolean forward = toVertexTime < 0;

                  // PRUNING : grow only forward
                  // or backward from rightmost vertex
                  if (forward || rightMostVertex) {

                    DFSEmbedding childEmbedding =
                      DFSEmbedding.deepCopy(parentEmbedding);
                    DFSCode childCode = DFSCode.deepCopy(parentCode);

                    // add new vertex to embedding for forward steps
                    if (forward) {
                      childEmbedding.getVertexTimes().add(toVertexId);
                      toVertexTime = childEmbedding.getVertexTimes().size() - 1;
                    }

                    childCode.getSteps().add(new DFSStep(
                      fromVertexTime,
                      fromVertexLabel,
                      entry.isOutgoing(),
                      entry.getEdgeLabel(),
                      toVertexTime, toVertexLabel
                    ));

                    childEmbedding.getEdgeTimes().add(edgeId);

                    CompressedDFSCode compressedChildCode =
                      new CompressedDFSCode(childCode);

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

  private static CompressedDFSCode minimumDfsCode(
    final Collection<CompressedDFSCode> dfsCodes, final FSMConfig fsmConfig) {

    Iterator<CompressedDFSCode> iterator = dfsCodes.iterator();

    DFSCode minCode = iterator.next().getDfsCode();

    for(CompressedDFSCode compressedDFSCode : dfsCodes) {
      DFSCode nextCode = compressedDFSCode.getDfsCode();

      if(getSiblingComparator(fsmConfig).compare(nextCode, minCode) < 0) {
        minCode = nextCode;
      }
    }

    return new CompressedDFSCode(minCode);
  }

  private static DfsCodeSiblingComparator getSiblingComparator(
    FSMConfig fsmConfig) {
    return new DfsCodeSiblingComparator(fsmConfig.isDirected());
  }

  private static Set<CompressedDFSCode> addSibling(
    Set<CompressedDFSCode> siblings, CompressedDFSCode code) {

    if (siblings == null) {
      siblings = Sets.newHashSet(code);
    } else {
      siblings.add(code);
    }

    return siblings;
  }

  private static Collection<Set<CompressedDFSCode>> addSiblings(
    Collection<Set<CompressedDFSCode>> siblingGroups,
    Set<CompressedDFSCode> siblings) {

    if(siblings != null) {
      if (siblingGroups == null) {
        siblingGroups = Lists.newArrayList();
      }
      siblingGroups.add(siblings);
    }

    return siblingGroups;
  }

  private static Map<CompressedDFSCode, Collection<DFSEmbedding>>
  addCodeEmbedding(
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings,
    CompressedDFSCode code, DFSEmbedding embedding) {

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

  public static boolean isValidMinimumDfsCode(
    CompressedDFSCode compressedDFSCode, FSMConfig fsmConfig) {

    GSpanTransaction transaction = createTransaction(compressedDFSCode);

    CompressedDFSCode minDfsCode = null;

    while (transaction.hasGrownSubgraphs()) {
      minDfsCode = minimumDfsCode(
        transaction.getCodeEmbeddings().keySet(), fsmConfig);

      growFrequentSubgraphs(
        transaction, Lists.newArrayList(minDfsCode), fsmConfig);
    }

    return compressedDFSCode.equals(minDfsCode);
  }

  private static void addEmbedding(Collection<DFSEmbedding> embeddings,
    int edgeId, DFSStep step, boolean inverse) {

    List<Integer> vertexTimes;

    if (step.isLoop()) {
      vertexTimes = Lists.newArrayList(step.getFromTime());
    } else if(!inverse) {
      vertexTimes = Lists.newArrayList(step.getFromTime(), step.getToTime());
    } else {
      vertexTimes = Lists.newArrayList(step.getToTime(), step.getFromTime());
    }

    List<Integer> edgeTimes = Lists.newArrayList(edgeId);
    embeddings.add(new DFSEmbedding(vertexTimes, edgeTimes));
  }

  private static boolean inversePattern(DFSStep firstStep, DFSStep secondStep) {
    return ! secondStep.isOutgoing().equals(firstStep.isOutgoing()) &&
      secondStep.isLoop().equals(firstStep.isLoop()) &&
      secondStep.getFromLabel().equals(firstStep.getToLabel()) &&
      secondStep.getToLabel().equals(firstStep.getFromLabel());
  }

  private static boolean samePattern(DFSStep firstStep, DFSStep secondStep) {
    return secondStep.isOutgoing().equals(firstStep.isOutgoing()) &&
      secondStep.isLoop().equals(firstStep.isLoop()) &&
      secondStep.getFromLabel().equals(firstStep.getFromLabel()) &&
      secondStep.getToLabel().equals(firstStep.getToLabel());
  }

  private static void addEntry(List<AdjacencyList> adjacencyLists,
    int fromId, int fromLabel, boolean outgoing, int edgeId, int edgeLabel,
    int toId, int toLabel) {

    AdjacencyList adjacencyList = adjacencyLists.get(fromId);

    AdjacencyListEntry entry =
      new AdjacencyListEntry(outgoing, edgeId, edgeLabel, toId, toLabel);

    if(adjacencyList == null) {
      adjacencyLists.add(fromId, new AdjacencyList(fromLabel, entry));
    } else {
      adjacencyList.getEntries().add(entry);
    }
  }

  private static Collection<Set<CompressedDFSCode>> initCodeSiblings(
    CompressedDFSCode startCode) {
    Set<CompressedDFSCode> siblings = Sets.newHashSet(startCode);
    Collection<Set<CompressedDFSCode>> codeSiblings =
      Lists.newArrayListWithExpectedSize(1);
    codeSiblings.add(siblings);
    return codeSiblings;
  }

  private static Map<CompressedDFSCode, Collection<DFSEmbedding>> initCodeEmbeddings(
    CompressedDFSCode startCode, Collection<DFSEmbedding> embeddings) {
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();
    codeEmbeddings.put(startCode, embeddings);
    return codeEmbeddings;
  }
}
