//package org.gradoop.model.impl.operators.tostring;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.gradoop.model.api.EPGMEdge;
//import org.gradoop.model.api.EPGMGraphHead;
//import org.gradoop.model.api.EPGMVertex;
//import org.gradoop.model.impl.GraphTransaction;
//import org.gradoop.model.impl.id.GradoopId;
//import org.gradoop.model.impl.id.GradoopIdSet;
//import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
//import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
//import org.gradoop.model.impl.operators.tostring.api.VertexToString;
//import org.gradoop.model.impl.operators.tostring.comparators.DfsCodeComparator;
//import org.gradoop.model.impl.operators.tostring.comparators
//  .EdgePatternComparator;
//import org.gradoop.model.impl.operators.tostring.functions
//  .EPGMElementToDataString;
//import org.gradoop.model.impl.operators.tostring.pojos.DFSCode;
//import org.gradoop.model.impl.operators.tostring.pojos.DFSEmbedding;
//import org.gradoop.model.impl.operators.tostring.pojos.DFSStep;
//import org.gradoop.model.impl.operators.tostring.pojos.EdgePattern;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//
//public class MinDFSCode
//  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
//  implements MapFunction<GraphTransaction<G, V, E>, String> {
//
//  private final EPGMElementToDataString<G> graphHeadToString;
//  private final EPGMElementToDataString<V> vertexToString;
//  private final EPGMElementToDataString<E> edgeToString;
//
//  public MinDFSCode(
//    EPGMElementToDataString<G> graphHeadToString,
//    EPGMElementToDataString<V> vertexToString,
//    EPGMElementToDataString<E> edgeToString) {
//
//    this.graphHeadToString = graphHeadToString;
//    this.vertexToString = vertexToString;
//    this.edgeToString = edgeToString;
//
//  }
//
//  @Override
//  public String map(GraphTransaction<G, V, E> transaction) throws
//    Exception {
//
//    Map<GradoopId, String> vertexLabels = Maps.newHashMapWithExpectedSize
//      (transaction.getVertices().size());
//
//    for(V vertex : transaction.getVertices()) {
//      vertexLabels.put(vertex.getId(), vertexToString.label(vertex));
//    }
//
//    Map<GradoopIdSet, Collection<DFSCode>> subgraphCodes = Maps
//      .newHashMap();
//    Map<DFSCode, Collection<DFSEmbedding>> codeEmbeddings = Maps
//      .newHashMap();
//
//    // SIZE 1 - grow from root
//
//    for(E edge : transaction.getEdges()) {
//
//      DFSStep step;
//      DFSEmbedding embedding;
//      GradoopIdSet subgraph;
//
//      GradoopId sourceId = edge.getSourceId();
//      GradoopId targetId = edge.getTargetId();
//
//      String sourceLabel = vertexLabels.get(sourceId);
//      String edgeLabel = edgeToString.label(edge);
//      String targetLabel = vertexLabels.get(targetId);
//
//      boolean loop = sourceId.equals(targetId);
//      boolean outgoing = sourceLabel.compareTo(targetLabel) <= 0;
//
//      if(loop) {
//        subgraph = GradoopIdSet.fromExisting(sourceId);
//        embedding = new DFSEmbedding(
//          Lists.newArrayList(sourceId), edge.getId());
//        step = new DFSStep(0, sourceLabel, true, edgeLabel, 0, sourceLabel);
//      } else {
//        subgraph = GradoopIdSet.fromExisting(sourceId, targetId);
//
//        if(outgoing) {
//          embedding = new DFSEmbedding(
//            Lists.newArrayList(sourceId, targetId), edge.getId());
//          step = new DFSStep(0, sourceLabel, true, edgeLabel, 1, targetLabel);
//        } else {
//          embedding = new DFSEmbedding(
//            Lists.newArrayList(targetId, sourceId), edge.getId());
//          step = new DFSStep(0, targetLabel, false, edgeLabel, 1, sourceLabel);
//        }
//      }
//
//      DFSCode childCode = new DFSCode(step);
//      Collection<DFSCode> codes = subgraphCodes.get(subgraph);
//
//      if(codes == null) {
//        subgraphCodes.put(subgraph, Lists.newArrayList(childCode));
//      } else {
//        codes.add(childCode);
//      }
//
//      Collection<DFSEmbedding> embeddings = codeEmbeddings.get(childCode);
//
//      if(embeddings == null) {
//        codeEmbeddings.put(childCode, Lists.newArrayList(embedding));
//      } else {
//        embeddings.add(embedding);
//      }
//    }
//
//    // SIZE 2..E
//
//    for(int i = 1; i< transaction.getEdges().size(); i++) {
//
//      // identify minimum DFS code
//
//      DFSCode parentDfsCode = getMinDfsCode(codeEmbeddings);
//
//      // grow embeddings of minimum DFS code
//
//      Collection<DFSEmbedding> parentCodeEmbeddings = codeEmbeddings
//        .get(parentDfsCode);
//
//      Map<DFSCode, Collection<DFSEmbedding>> childEmbeddings =
//        Maps.newHashMap();
//
//      assert parentDfsCode != null;
//      EdgePattern minPattern = parentDfsCode.getMinEdgePattern();
//      List<Integer> rightmostPath = parentDfsCode.getRightMostPathVertexTimes();
//
//      for(DFSEmbedding parentEmbedding : parentCodeEmbeddings) {
//        // rightmost path is inverse, so first element is rightmost vertex
//        Boolean rightMostVertex = true;
//        ArrayList<GradoopId> vertexTimes = parentEmbedding.getVertexTimes();
//
//        // for each time on rightmost path
//        for (Integer fromVertexTime : rightmostPath) {
//
//          GradoopId fromVertexId = vertexTimes.get(fromVertexTime);
//          String fromVertexLabel = vertexLabels.get(fromVertexId);
//
//          // for each incident edge
//          for (E edge : transaction.getEdges()) {
//            boolean isSourceVertex = edge.getSourceId().equals(fromVertexId);
//            boolean isTargetVertex = edge.getTargetId().equals(fromVertexId);
//
//            if(isSourceVertex || isTargetVertex) {
//
//              GradoopId edgeId = edge.getId();
//
//              // allow only edges not already contained
//              if (!parentEmbedding.getEdgeTimes().contains(edgeId)) {
//                boolean outgoing = isSourceVertex;
//
//                GradoopId toVertexId =
//                  outgoing ? edge.getTargetId() : edge.getSourceId();
//
//                String edgeLabel = edgeToString.label(edge);
//                String toVertexLabel = vertexLabels.get(toVertexId);
//
//                EdgePattern candidatePattern = new EdgePattern(
//                  fromVertexLabel, outgoing, edgeLabel, toVertexLabel);
//
//                // PRUNING : continue only if edge pattern is lexicographically
//                // larger than first step of DFS code
//                EdgePatternComparator edgePatternComparator = new
//                  EdgePatternComparator(true);
//
//                if (edgePatternComparator
//                  .compare(minPattern, candidatePattern) <= 0) {
//
//                  // query toVertexData
//                  Integer toVertexTime = vertexTimes.indexOf(toVertexId);
//                  boolean forward = toVertexTime < 0;
//
//                  // PRUNING : grow only forward
//                  // or backward from rightmost vertex
//                  if (forward || rightMostVertex) {
//
//                    DFSEmbedding childEmbedding = DFSEmbedding
//                      .deepCopy(parentEmbedding);
//                    DFSCode childCode = DFSCode
//                      .deepCopy(parentDfsCode);
//
//                    // add new vertex to embedding for forward steps
//                    if (forward) {
//                      childEmbedding.getVertexTimes().add(toVertexId);
//                      toVertexTime = childEmbedding.getVertexTimes().size() - 1;
//                    }
//
//                    childCode.getSteps().add(new DFSStep(
//                      fromVertexTime,
//                      fromVertexLabel,
//                      outgoing,
//                      edgeLabel,
//                      toVertexTime,
//                      toVertexLabel
//                    ));
//
//                    childEmbedding.getEdgeTimes().add(edgeId);
//
//                    Collection<DFSEmbedding> embeddings =
//                      childEmbeddings.get(childCode);
//
//                    if(embeddings == null) {
//                      childEmbeddings.put(
//                        childCode, Lists.newArrayList(childEmbedding));
//                    } else {
//                      embeddings.add(childEmbedding);
//                    }
//                  }
//                }
//              }
//            }
//          }
//          rightMostVertex = false;
//        }
//      }
//      if(childEmbeddings.isEmpty()) {
//        break;
//      } else {
//        codeEmbeddings = childEmbeddings;
//      }
//    }
//
//    return graphHeadToString.label(transaction.getGraphHead()) +
//      getMinDfsCode(codeEmbeddings).toString();
//  }
//
//  private DFSCode getMinDfsCode(
//    Map<DFSCode, Collection<DFSEmbedding>> codeEmbeddings) {
//
//    DFSCode minDfsCode = null;
//
//    for(DFSCode code : codeEmbeddings.keySet()) {
//      DfsCodeComparator dfsCodeComparator = new DfsCodeComparator(true);
//      if(minDfsCode == null ||
//        dfsCodeComparator.compare(code, minDfsCode) < 0) {
//        minDfsCode = code;
//      }
//    }
//
//    return minDfsCode;
//  }
//}
