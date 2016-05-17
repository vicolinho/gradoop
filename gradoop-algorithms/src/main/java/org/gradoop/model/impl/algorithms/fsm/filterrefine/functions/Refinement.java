package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.DfsCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.StepPatternMapping;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.StepPattern;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.StepMappings;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Refinement implements
  FlatJoinFunction<Tuple2<Integer, Collection<CompressedDFSCode>>,
    Tuple2<Integer, Map<Integer, Transaction>>, CompressedDFSCode> {

  private final DfsCodeComparator comparator;
  private final boolean directed;

  public Refinement(FSMConfig config) {
    directed = config.isDirected();
    this.comparator = new DfsCodeComparator(directed);
  }

  @Override
  public void join(
    Tuple2<Integer, Collection<CompressedDFSCode>> codePair,
    Tuple2<Integer, Map<Integer, Transaction>> transactionPair,
    Collector<CompressedDFSCode> collector) throws Exception {

    List<CompressedDFSCode> compressedCodes = Lists.newArrayList();
    List<DFSCode> codes = Lists.newArrayList();
    List<DFSEmbedding> emptyEmbeddings = Lists.newArrayList();
    Set<StepPattern> allStepPatterns = Sets.newHashSet();

    for(CompressedDFSCode compressedDFSCode : codePair.f1) {
      compressedCodes.add(compressedDFSCode);

      DFSCode code = compressedDFSCode.getDfsCode();
      codes.add(code);

      ArrayList<Integer> edgeTimes = Lists
        .newArrayListWithCapacity(code.size());

      Integer maxVertexTime = 0;

      for(DFSStep step : code.getSteps()) {
        StepPattern stepPattern = new StepPattern(step);
        allStepPatterns.add(stepPattern);
        edgeTimes.add(null);

        if(step.isForward() && step.getToTime() > maxVertexTime) {
          maxVertexTime = step.getToTime();
        }
      }

      ArrayList<Integer> vertexTimes = Lists
        .newArrayListWithCapacity(maxVertexTime + 1);

      for(int i = 0; i <= maxVertexTime; i++) {
        vertexTimes.add(null);
      }

      emptyEmbeddings.add(new DFSEmbedding(vertexTimes, edgeTimes));
    }

    // for each graph
    for(Transaction graph : transactionPair.f1.values()) {

      // index step pattern mappings
      Map<StepPattern, Collection<StepPatternMapping>> stepPatternMappings =
        findStepPatternMappings(graph, allStepPatterns);

      // for each DFS code
      int codeIndex = 0;
      for(DFSCode code : codes) {
        boolean matchPossible = true;

        // check, if candidates are available for each step

        List<StepMappings> stepMappings = Lists
          .newArrayListWithCapacity(code.size());

        int edgeTime = 0;
        for(DFSStep step : code.getSteps()) {
          StepPattern pattern = new StepPattern(step);

          Collection<StepPatternMapping> patternMappings = stepPatternMappings
            .get(pattern);

          if(patternMappings == null) {
            matchPossible = false;
            break;
          } else {
            stepMappings.add(new StepMappings(step, patternMappings, edgeTime));
          }
          edgeTime++;
        }

        // if candidates

        if(matchPossible) {
          Collections.sort(stepMappings);

          Collection<DFSEmbedding> embeddings =
            Lists.newArrayList(emptyEmbeddings.get(codeIndex));

          Iterator<StepMappings> iterator = stepMappings.iterator();

          while (!embeddings.isEmpty() && iterator.hasNext()) {
            StepMappings stepWithMappings = iterator.next();

            embeddings = growByStep(embeddings, stepWithMappings);
          }

          // match
          if(! embeddings.isEmpty()) {
            CompressedDFSCode compressedDFSCode =
              compressedCodes.get(codeIndex);
            compressedDFSCode.setSupport(compressedDFSCode.getSupport() + 1);
          }
        }
        codeIndex++;
      }
    }
    for(CompressedDFSCode code : compressedCodes) {
      if(code.getSupport() > 0) {
        collector.collect(code);
      }
    }
  }

  private Collection<DFSEmbedding> growByStep(
    Collection<DFSEmbedding> parents, StepMappings stepWithMappings) {
    Collection<DFSEmbedding> children = Lists.newArrayList();

    DFSStep step = stepWithMappings.getStep();
    Collection<StepPatternMapping> mappings = stepWithMappings.getMappings();

    for (DFSEmbedding parent : parents) {
      for (StepPatternMapping mapping : mappings)
      {
        if(! parent.getEdgeTimes().contains(mapping.getEdgeId())) {
          Integer sourceTime =
            step.isOutgoing() ? step.getFromTime() : step.getToTime();
          Integer mappingSourceId = mapping.getSourceId();

          Integer mappedSourceId = parent.getVertexTimes().get(sourceTime);

          if(mappedSourceId == null || mappedSourceId.equals(mappingSourceId)) {
            int targetTime =
              step.isOutgoing() ? step.getToTime() : step.getFromTime();
            Integer mappingTargetId = mapping.getTargetId();
            Integer mappedTargetId = parent.getVertexTimes().get(targetTime);

            if(mappedTargetId == null || mappedTargetId.equals(mappingTargetId)) {
              int edgeTime = stepWithMappings.getEdgeTime();

              DFSEmbedding child = DFSEmbedding.deepCopy(parent);

              child.getVertexTimes().set(sourceTime, mappingSourceId);
              child.getVertexTimes().set(targetTime, mappingTargetId);
              child.getEdgeTimes().set(edgeTime, mapping.getEdgeId());

              children.add(child);
            }
          }
        }
      }
    }
    return children;
  }


  private Map<StepPattern, Collection<StepPatternMapping>>
  findStepPatternMappings( Transaction graph, Set<StepPattern> allStepPatterns)
  {
    // identify candidates

    Map<StepPattern, Collection<StepPatternMapping>> stepPatternMappings = Maps
      .newHashMapWithExpectedSize(allStepPatterns.size());

    // for each vertex
    int sourceVertexId = 0;
    for(AdjacencyList vertex : graph.getAdjacencyLists()) {

      // for each (outgoing) edge
      for(AdjacencyListEntry edge : vertex.getEntries()) {
        if (edge.isOutgoing() || !directed) {

          StepPattern pattern = new StepPattern(
            vertex.getVertexLabel(),
            edge.getEdgeLabel(),
            edge.getVertexLabel()
          );

          if (allStepPatterns.contains(pattern))
          {
            StepPatternMapping mapping = new StepPatternMapping(
              sourceVertexId, edge.getEdgeId(), edge.getVertexId());

            Collection<StepPatternMapping> mappings = stepPatternMappings
              .get(pattern);

            if(mappings == null) {
              mappings = Lists.newArrayList(mapping);
              stepPatternMappings.put(pattern, mappings);
            } else {
              mappings.add(mapping);
            }
          }
        }
      }
      sourceVertexId++;
    }
    return stepPatternMappings;
  }


}
