//package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//import org.apache.flink.api.common.functions.FlatJoinFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.util.Collector;
//import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
//import org.gradoop.model.impl.algorithms.fsm.common.pojos.*;
//import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
//import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;
//import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.StepMappings;
//import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.StepPattern;
//import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.StepPatternMapping;
//
//import java.util.*;
//
//public class Refinement implements
//  FlatJoinFunction<Tuple2<Integer, Collection<CompressedDfsCode>>,
//    Tuple2<Integer, Collection<GSpanTransaction>>, Supportable<CompressedDfsCode>> {
//
//  private final FSMConfig fsmConfig;
//
//  public Refinement(FSMConfig config) {
//    fsmConfig = config;
//  }
//
//  @Override
//  public void join(
//    Tuple2<Integer, Collection<CompressedDfsCode>> codePair,
//    Tuple2<Integer, Collection<GSpanTransaction>> transactionPair,
//    Collector<Supportable<CompressedDfsCode>> collector) throws Exception {
//
//    List<CompressedDfsCode> compressedCodes = Lists.newArrayList();
//    List<DfsCode> codes = Lists.newArrayList();
//    List<DFSEmbedding> emptyEmbeddings = Lists.newArrayList();
//    Set<StepPattern> allStepPatterns = Sets.newHashSet();
//
//    for(CompressedDfsCode subgraph : codePair.f1) {
//      compressedCodes.add(subgraph);
//
//      DfsCode code = subgraph.getDfsCode();
//      codes.add(code);
//
//      ArrayList<Integer> edgeTimes = Lists
//        .newArrayListWithCapacity(code.size());
//
//      Integer maxVertexTime = 0;
//
//      for(DFSStep step : code.getSteps()) {
//        StepPattern stepPattern = new StepPattern(step);
//        allStepPatterns.add(stepPattern);
//        edgeTimes.add(null);
//
//        if(step.isForward() && step.getToTime() > maxVertexTime) {
//          maxVertexTime = step.getToTime();
//        }
//      }
//
//      ArrayList<Integer> vertexTimes = Lists
//        .newArrayListWithCapacity(maxVertexTime + 1);
//
//      for(int i = 0; i <= maxVertexTime; i++) {
//        vertexTimes.add(null);
//      }
//
//      emptyEmbeddings.add(new DFSEmbedding(vertexTimes, edgeTimes));
//    }
//
//    // for each graph
//    for(GSpanTransaction graph : transactionPair.f1) {
//
//      // index step pattern mappings
//      Map<StepPattern, Collection<StepPatternMapping>> stepPatternMappings =
//        findStepPatternMappings(graph, allStepPatterns);
//
//      // for each DFS code
//      int codeIndex = 0;
//      for(DfsCode code : codes) {
//        boolean matchPossible = true;
//
//        // check, if candidates are available for each step
//
//        List<StepMappings> stepMappings = Lists
//          .newArrayListWithCapacity(code.size());
//
//        int edgeTime = 0;
//        for(DFSStep step : code.getSteps()) {
//          StepPattern pattern = new StepPattern(step);
//
//          Collection<StepPatternMapping> patternMappings = stepPatternMappings
//            .get(pattern);
//
//          if(patternMappings == null) {
//            matchPossible = false;
//            break;
//          } else {
//            stepMappings.add(new StepMappings(step, patternMappings, edgeTime));
//          }
//          edgeTime++;
//        }
//
//        // if candidates
//
//        if(matchPossible) {
//          Collections.sort(stepMappings);
//
//          Collection<DFSEmbedding> embeddings =
//            Lists.newArrayList(emptyEmbeddings.get(codeIndex));
//
//          Iterator<StepMappings> iterator = stepMappings.iterator();
//
//          while (!embeddings.isEmpty() && iterator.hasNext()) {
//            StepMappings stepWithMappings = iterator.next();
//
//            embeddings = growByStep(embeddings, stepWithMappings);
//          }
//
//          // match
//          if(! embeddings.isEmpty()) {
//            CompressedDfsCode subgraph =
//              compressedCodes.get(codeIndex);
//            subgraph.setSupport(subgraph.getSupport() + 1);
//          }
//        }
//        codeIndex++;
//      }
//    }
//    for(Supportable<CompressedDfsCode> code : compressedCodes) {
//      if(code.getSupport() > 0) {
//        collector.collect(code);
//      }
//    }
//  }
//
//  private Collection<DFSEmbedding> growByStep(
//    Collection<DFSEmbedding> parents, StepMappings stepWithMappings) {
//    Collection<DFSEmbedding> children = Lists.newArrayList();
//
//    DFSStep step = stepWithMappings.getStep();
//    Collection<StepPatternMapping> mappings = stepWithMappings.getMappings();
//
//    for (DFSEmbedding parent : parents) {
//      for (StepPatternMapping mapping : mappings)
//      {
//        if(! parent.getEdgeTimes().contains(mapping.getEdgeId())) {
//          Integer sourceTime =
//            step.isOutgoing() ? step.getFromTime() : step.getToTime();
//          Integer mappingSourceId = mapping.getSourceId();
//
//          Integer mappedSourceId = parent.getVertexTimes().get(sourceTime);
//
//          if(mappedSourceId == null || mappedSourceId.equals(mappingSourceId)) {
//            int targetTime =
//              step.isOutgoing() ? step.getToTime() : step.getFromTime();
//            Integer mappingTargetId = mapping.getTargetId();
//            Integer mappedTargetId = parent.getVertexTimes().get(targetTime);
//
//            if(mappedTargetId == null || mappedTargetId.equals(mappingTargetId)) {
//              int edgeTime = stepWithMappings.getEdgeTime();
//
//              DFSEmbedding child = DFSEmbedding.deepCopy(parent);
//
//              child.getVertexTimes().set(sourceTime, mappingSourceId);
//              child.getVertexTimes().set(targetTime, mappingTargetId);
//              child.getEdgeTimes().set(edgeTime, mapping.getEdgeId());
//
//              children.add(child);
//            }
//          }
//        }
//      }
//    }
//    return children;
//  }
//
//  private Map<StepPattern, Collection<StepPatternMapping>>
//  findStepPatternMappings(
//    GSpanTransaction graph, Set<StepPattern> allStepPatterns)
//  {
//    // identify candidates
//
//    Map<StepPattern, Collection<StepPatternMapping>> stepPatternMappings = Maps
//      .newHashMapWithExpectedSize(allStepPatterns.size());
//
//    // for each vertex
//    int sourceVertexId = 0;
//    for(AdjacencyList vertex : graph.getAdjacencyLists()) {
//
//      // for each (outgoing) edge
//      for(AdjacencyListEntry edge : vertex.getEntries()) {
//        if (edge.isOutgoing() || !fsmConfig.isDirected()) {
//
//          StepPattern pattern = new StepPattern(
//            vertex.getFromVertexLabel(),
//            edge.getEdgeLabel(),
//            edge.getToVertexLabel()
//          );
//
//          if (allStepPatterns.contains(pattern))
//          {
//            StepPatternMapping mapping = new StepPatternMapping(
//              sourceVertexId, edge.getEdgeId(), edge.getToVertexId());
//
//            Collection<StepPatternMapping> mappings = stepPatternMappings
//              .get(pattern);
//
//            if(mappings == null) {
//              mappings = Lists.newArrayList(mapping);
//              stepPatternMappings.put(pattern, mappings);
//            } else {
//              mappings.add(mapping);
//            }
//          }
//        }
//      }
//      sourceVertexId++;
//    }
//    return stepPatternMappings;
//  }
//
//
//}
