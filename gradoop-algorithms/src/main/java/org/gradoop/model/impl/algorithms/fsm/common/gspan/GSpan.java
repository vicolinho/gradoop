package org.gradoop.model.impl.algorithms.fsm.common.gspan;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.IntegerEdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples
  .IntegerLabeledEdgeTriple;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GSpan {

  public static GSpanTransaction createTransaction(
    Iterable<Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>>
      iterable, FSMConfig fsmConfig) {

    Map<DFSCode, Collection<IntegerEdgeTriple>> codeEdges = createCodeEdges(iterable);

    Map<Integer, AdjacencyList> adjacencyLists = com.google.common.collect.Maps.newHashMap();
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      com.google.common.collect.Maps.newHashMap();


    List<DFSCode> oneEdgeCodes = Lists.newArrayList(codeEdges.keySet());

    Collections.sort(oneEdgeCodes, getSiblingComparator(fsmConfig));

    int minEdgePatternId = 0;

    Collection<Collection<CompressedDFSCode>> codeSiblings =
      Lists.newArrayList();

    for(DFSCode code : oneEdgeCodes) {
      Collection<DFSEmbedding> embeddings = Lists.newArrayList();

      for(IntegerEdgeTriple edge : codeEdges.get(code)) {

        updateAdjacencyLists(adjacencyLists, edge, minEdgePatternId);

        List<Integer> vertexTimes = edge.isLoop() ?
          Lists.newArrayList(edge.getSourceId()) :
          Lists.newArrayList(edge.getSourceId(), edge.getTargetId());

        List<Integer> edgeTimes = Lists.newArrayList(edge.getEdgeId());

        embeddings.add(
          new DFSEmbedding(minEdgePatternId, vertexTimes, edgeTimes));
      }

      CompressedDFSCode compressedCode = new CompressedDFSCode(code);

      codeEmbeddings.put(compressedCode, embeddings);
      codeSiblings.add(Lists.newArrayList(compressedCode));

      minEdgePatternId++;
    }

    return new GSpanTransaction(adjacencyLists, codeEmbeddings, codeSiblings);
  }

  private static Map<DFSCode, Collection<IntegerEdgeTriple>> createCodeEdges(
    Iterable<Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>>
      iterable) {
    Map<DFSCode, Collection<IntegerEdgeTriple>> codeEdges = Maps.newHashMap();

    Map<GradoopId, Integer> vertexMap = new HashMap<>();
    int vertexId = 0;
    int edgeId = 0;

    for(Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>
      triple : iterable) {

      IntegerLabeledEdgeTriple fatEdge = triple.f1;
      DFSCode code = triple.f2.getDfsCode();

      GradoopId minGradoopId = fatEdge.getSourceId();
      Integer minId = vertexMap.get(minGradoopId);
      if(minId == null) {
        minId = vertexId;
        vertexMap.put(minGradoopId, minId);
        vertexId++;
      }

      GradoopId maxGradoopId = fatEdge.getTargetId();
      Integer maxId = vertexMap.get(maxGradoopId);
      if(maxId == null) {
        maxId = vertexId;
        vertexMap.put(maxGradoopId, maxId);
        vertexId++;
      }

      IntegerEdgeTriple integerEdgeTriple = new IntegerEdgeTriple(
        minId, fatEdge.getTargetLabel(),
        edgeId, fatEdge.getLabel(),
        maxId, fatEdge.getTargetLabel()
      );

      Collection<IntegerEdgeTriple> siblings = codeEdges.get(code);

      if (siblings == null) {
        codeEdges.put(code, Lists.newArrayList(integerEdgeTriple));
      } else {
        siblings.add(integerEdgeTriple);
      }

      edgeId++;
    }
    return codeEdges;
  }

  private static void updateAdjacencyLists(
    Map<Integer, AdjacencyList> adjacencyLists, IntegerEdgeTriple edge,
    int minEdgePatternId) {

    int minId = edge.getSourceId();
    int minLabel = edge.getSourceLabel();

    int edgeId = edge.getEdgeId();
    boolean loop = edge.isLoop();
    int edgeLabel = edge.getLabel();

    int maxId = edge.getTargetId();
    int maxLabel = edge.getTargetLabel();


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
      minEdgePatternId, true, edgeId, edgeLabel, maxId, maxLabel));

    maxAdjacencyList.getEntries().add(new AdjacencyListEntry(
      minEdgePatternId, false, edgeId, edgeLabel, minId, minLabel));
  }

  public static void growFrequentSubgraphs(final GSpanTransaction transaction,
    Collection<CompressedDFSCode> frequentDfsCodes, FSMConfig fsmConfig) {

    Map<CompressedDFSCode, Collection<DFSEmbedding>> childCodeEmbeddings = null;
    Collection<Collection<CompressedDFSCode>> childSiblingGroups = null;

    // for each leaf on leftmost branch in DFS code tree
    for (Collection<CompressedDFSCode> parentSiblings :
      transaction.getSiblingGroups()) {

      if (!parentSiblings.isEmpty()) {
        Collection<CompressedDFSCode> childSiblings = null;

        DFSCode parentCode = findMinimumSupportedFrequentDfsCode(
          parentSiblings, frequentDfsCodes, fsmConfig);

        List<Integer> rightmostPath = parentCode.getRightMostPathVertexTimes();

        // for each embedding
        for (DFSEmbedding parentEmbedding : transaction.getCodeEmbeddings()
          .get(new CompressedDFSCode(parentCode))) {

          // first iterated vertex is rightmost
          Boolean rightMostVertex = true;
          List<Integer> vertexTimes = parentEmbedding.getVertexTimes();

          // for each time on rightmost path
          for (Integer fromVertexTime : rightmostPath) {

            // query fromVertex data
            AdjacencyList adjacencyList = transaction.getAdjacencyLists()
              .get(vertexTimes.get(fromVertexTime));
            Integer fromVertexLabel = adjacencyList.getFromVertexLabel();

            // for each incident edge
            for (AdjacencyListEntry entry : adjacencyList.getEntries()) {

              // if valid extension for branch
              if (entry.getMinEdgePatternId() >=
                parentEmbedding.getMinEdgePatternId()) {

                Integer edgeId = entry.getEdgeId();

                // if edge not already contained
                if (!parentEmbedding.getEdgeTimes()
                  .contains(edgeId)) {
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
                      toVertexTime,
                      entry.getToVertexLabel()
                    ));

                    childEmbedding.getEdgeTimes().add(edgeId);

                    CompressedDFSCode compressedChildCode =
                      new CompressedDFSCode(childCode);

                    childSiblings = addSibling(
                      childSiblings, compressedChildCode);

                    childCodeEmbeddings = addCodeEmbedding(
                      childCodeEmbeddings, compressedChildCode, childEmbedding);
                  }
                }
              }
            }
            rightMostVertex = false;
          }
        }
        childSiblingGroups = addSiblings(childSiblingGroups, childSiblings);
      }
    }

    transaction.setCodeEmbeddings(childCodeEmbeddings);
    transaction.setCodeSiblings(childSiblingGroups);

  }

  private static DFSCode findMinimumSupportedFrequentDfsCode(
    final Collection<CompressedDFSCode> dfsCodes,
    Collection<CompressedDFSCode> frequentDfsCodes, final FSMConfig fsmConfig) {

    Iterator<CompressedDFSCode> iterator = dfsCodes.iterator();

    DFSCode minCode = iterator.next().getDfsCode();

    for(CompressedDFSCode compressedDFSCode : dfsCodes) {
      if(frequentDfsCodes == null ||
        frequentDfsCodes.contains(compressedDFSCode)) {

        DFSCode nextCode = compressedDFSCode.getDfsCode();

        if(minCode == null ||
          getSiblingComparator(fsmConfig).compare(nextCode, minCode) < 0) {
          minCode = nextCode;
        }
      }
    }

    return minCode;
  }

  private static DfsCodeSiblingComparator getSiblingComparator(
    FSMConfig fsmConfig) {
    return new DfsCodeSiblingComparator(fsmConfig.isDirected());
  }

  private static Collection<CompressedDFSCode> addSibling(
    Collection<CompressedDFSCode> siblings, CompressedDFSCode code) {

    if (siblings == null) {
      siblings = Lists.newArrayList(code);
    } else {
      siblings.add(code);
    }

    return siblings;
  }

  private static Collection<Collection<CompressedDFSCode>> addSiblings(
    Collection<Collection<CompressedDFSCode>> siblingGroups,
    Collection<CompressedDFSCode> siblings) {

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


  public static void prune(final GSpanTransaction transaction,
    final Collection<CompressedDFSCode> frequentDfsCodes) {

    Iterator<Collection<CompressedDFSCode>> groupIterator = transaction
      .getSiblingGroups().iterator();

    while (groupIterator.hasNext()) {
      Collection<CompressedDFSCode> group = groupIterator.next();
      Iterator<CompressedDFSCode> codeIterator = group.iterator();

      while (codeIterator.hasNext()) {
        CompressedDFSCode code = codeIterator.next();

        if(! frequentDfsCodes.contains(code)) {
          // drop embeddings
          transaction.getCodeEmbeddings().remove(code);
          // remove from siblings
          codeIterator.remove();
        }
      }

      if(group.isEmpty()) {
        // drop empty sibling group
        groupIterator.remove();
      }
    }
  }

  public static boolean isValidMinimumDfsCode(
    CompressedDFSCode compressedDFSCode, FSMConfig fsmConfig) {

    DFSCode reportedCode = compressedDFSCode.getDfsCode();
    List<DFSStep> steps = reportedCode.getSteps();
    GSpanTransaction transaction = initTransaction(steps);

    for (int edgeCount = 2; edgeCount <= steps.size(); edgeCount++) {
      GSpan.growFrequentSubgraphs(transaction, null, fsmConfig);
    }

    DFSCode minDfsCode = GSpan.findMinimumSupportedFrequentDfsCode(
      transaction.getSiblingGroups().iterator().next(), null, fsmConfig);

    return reportedCode.equals(minDfsCode);
  }

  private static GSpanTransaction initTransaction(List<DFSStep> steps) {
    DFSStep firstStep = steps.get(0);

    CompressedDFSCode startCode = new CompressedDFSCode(new DFSCode(firstStep));

    Collection<DFSEmbedding> embeddings = Lists.newArrayList();

    Map<Integer, AdjacencyList> adjacencyLists = Maps.newHashMap();

    int edgeId = 0;
    for(DFSStep step : steps) {
      // first step or same direction and labels as first step
      if(edgeId == 0 || samePattern(firstStep, step)){
        addEmbedding(embeddings, edgeId, step, false);
        // inverse direction but same labels as first step
      } else if (inversePattern(firstStep, step)) {
        addEmbedding(embeddings, edgeId, step, true);
      }

      int fromId = step.getFromTime();
      int fromLabel = step.getFromLabel();
      boolean outgoing = step.isOutgoing();
      int edgeLabel = step.getEdgeLabel();
      int toId = step.getToTime();
      int toLabel = step.getToLabel();

      addEntry(adjacencyLists,
        fromId, fromLabel, outgoing, edgeId, edgeLabel, toId, toLabel);
      addEntry(adjacencyLists,
        toId, toLabel, !outgoing, edgeId, edgeLabel, fromId, fromLabel);

      edgeId++;
    }

    return new GSpanTransaction(adjacencyLists,
      initCodeEmbeddings(startCode, embeddings), initCodeSiblings(startCode));
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

  private static void addEntry(Map<Integer, AdjacencyList> adjacencyLists,
    int fromId, int fromLabel, boolean outgoing, int edgeId, int edgeLabel,
    int toId, int toLabel) {

    AdjacencyList adjacencyList = adjacencyLists.get(fromId);

    AdjacencyListEntry entry =
      new AdjacencyListEntry(outgoing, edgeId, edgeLabel, toId, toLabel);

    if(adjacencyList == null) {
      adjacencyLists.put(fromId, new AdjacencyList(fromLabel, entry));
    } else {
      adjacencyList.getEntries().add(entry);
    }
  }

  private static Collection<Collection<CompressedDFSCode>> initCodeSiblings(
    CompressedDFSCode startCode) {
    Collection<CompressedDFSCode> siblings = Lists
      .newArrayList(startCode);
    Collection<Collection<CompressedDFSCode>> codeSiblings =
      Lists.newArrayListWithExpectedSize(1);
    codeSiblings.add(siblings);
    return codeSiblings;
  }

  private static Map<CompressedDFSCode, Collection<DFSEmbedding>> initCodeEmbeddings(
    CompressedDFSCode startCode, Collection<DFSEmbedding> embeddings) {
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      com.google.common.collect.Maps.newHashMap();
    codeEmbeddings.put(startCode, embeddings);
    return codeEmbeddings;
  }
}
