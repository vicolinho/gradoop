package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.comparators.DfsCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.comparators.EdgePatternComparator;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsEmbedding;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsStep;
import org.gradoop.model.impl.algorithms.fsm.pojos.EdgePattern;
import org.gradoop.model.impl.algorithms.fsm.tuples.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GrowEmbeddings implements CrossFunction
  <SearchSpaceItem, CompressedDfsCode[], SearchSpaceItem> {

  private final DfsCodeComparator dfsCodeComparator;
  private final EdgePatternComparator edgePatternComparator;

  public GrowEmbeddings(FSMConfig fsmConfig) {
    boolean directed = fsmConfig.isDirected();

    this.dfsCodeComparator = new DfsCodeComparator(directed);
    this.edgePatternComparator = new EdgePatternComparator(directed);
  }

  @Override
  public SearchSpaceItem cross(SearchSpaceItem searchSpaceItem,
    CompressedDfsCode[] frequentDfsCodes) throws Exception {

    if(searchSpaceItem.isCollector()) {
      searchSpaceItem = collect(searchSpaceItem, frequentDfsCodes);
    } else {
      searchSpaceItem = grow(searchSpaceItem, frequentDfsCodes);
    }
    return searchSpaceItem;
  }

  private SearchSpaceItem collect(SearchSpaceItem collector,
    CompressedDfsCode[] newFrequentDfsCodes) {

    CompressedDfsCode[] pastFrequentDfsCodes = collector.getFrequentDfsCodes();

    CompressedDfsCode[] allFrequentDfsCode = new
      CompressedDfsCode[pastFrequentDfsCodes.length + newFrequentDfsCodes.length];

    int i = 0;

    for(CompressedDfsCode code : pastFrequentDfsCodes) {
      allFrequentDfsCode[i] = code;
      i++;
    }

    for(CompressedDfsCode code : newFrequentDfsCodes) {
      allFrequentDfsCode[i] = code;
      i++;
    }

    collector.setFrequentDfsCodes(allFrequentDfsCode);

    return collector;
  }

  private SearchSpaceItem grow(SearchSpaceItem graph,
    CompressedDfsCode[] frequentDfsCodes) {

    // min DFS code per subgraph (set of edge ids)
    Map<Integer, ArrayList<DfsCode>> coverageDfsCodes = new HashMap<>();
    Map<DfsCode, HashSet<DfsEmbedding>> codeEmbeddings = new HashMap<>();

    // for each supported DFS code
    for(Map.Entry<CompressedDfsCode, HashSet<DfsEmbedding>> entry :
      graph.getCodeEmbeddings().entrySet()) {

      CompressedDfsCode compressedDfsCode = entry.getKey();

      // PRUNING : grow only embeddings of frequent DFS codes
      if(ArrayUtils.contains(frequentDfsCodes, compressedDfsCode)) {

        DfsCode parentDfsCode = compressedDfsCode.getDfsCode();
        EdgePattern minPattern = parentDfsCode.getMinEdgePattern();
        List<Integer> rightmostPath = parentDfsCode.getRightMostPath();

        // for each embedding
        for(DfsEmbedding parentEmbedding : entry.getValue()) {

          // rightmost path is inverse, so first element is rightmost vertex
          Boolean rightMostVertex = true;
          ArrayList<GradoopId> vertexTimes = parentEmbedding.getVertexTimes();

          // for each time on rightmost path
          for(Integer fromVertexTime : rightmostPath) {

            // query fromVertex data
            AdjacencyList adjacencyList = graph.getAdjacencyLists()
              .get(vertexTimes.get(fromVertexTime));
            String fromVertexLabel = adjacencyList.getVertexLabel();

            // for each incident edge
            for(AdjacencyListEntry adjacencyListEntry :
              adjacencyList.getEntries()) {

              boolean outgoing = adjacencyListEntry.isOutgoing();
              String edgeLabel = adjacencyListEntry.getEdgeLabel();
              String toVertexLabel = adjacencyListEntry.getVertexLabel();

              EdgePattern candidatePattern = new EdgePattern(
                fromVertexLabel, outgoing, edgeLabel, toVertexLabel);

              // PRUNING : continue only if edge pattern is lexicographically
              // larger than first step of DFS code
              if(edgePatternComparator
                .compare(minPattern, candidatePattern) <= 0) {

                GradoopId edgeId = adjacencyListEntry.getEdgeId();

                // allow only edges not already contained
                if(!parentEmbedding.getEdgeTimes().contains(edgeId)) {


                  // query toVertexData
                  GradoopId toVertexId = adjacencyListEntry.getVertexId();
                  Integer toVertexTime = vertexTimes.indexOf(toVertexId);
                  boolean forward = toVertexTime < 0;

                  // PRUNING : grow only forward
                  // or backward from rightmost vertex
                  if(forward || rightMostVertex) {

                    DfsEmbedding embedding = DfsEmbedding
                      .deepCopy(parentEmbedding);
                    DfsCode dfsCode = DfsCode
                      .deepCopy(parentDfsCode);

                    // add new vertex to embedding for forward steps
                    if(forward) {
                      embedding.getVertexTimes().add(toVertexId);
                      toVertexTime = embedding.getVertexTimes().size() - 1;
                    }

                    dfsCode.getSteps().add(new DfsStep(
                      fromVertexTime,
                      fromVertexLabel,
                      outgoing,
                      edgeLabel,
                      toVertexTime,
                      toVertexLabel
                    ));

                    embedding.getEdgeTimes().add(edgeId);

                    // check if subgraph already discovered
                    Integer coverage = GradoopIdSet
                      .fromExisting(embedding.getEdgeTimes()).hashCode();

                    ArrayList<DfsCode> dfsCodes =
                      coverageDfsCodes.get(coverage);

                    if(dfsCodes == null) {
                      coverageDfsCodes.put(
                        coverage, Lists.newArrayList(dfsCode));
                    } else {
                      dfsCodes.add(dfsCode);
                    }

                    HashSet<DfsEmbedding> embeddings =
                      codeEmbeddings.get(dfsCode);

                    if(embeddings == null) {
                      codeEmbeddings.put(
                        dfsCode, Sets.newHashSet(embedding));
                    } else {
                      embeddings.add(embedding);
                    }
                  }
                }
              }
            }
            rightMostVertex = false;
          }
        }
      }
    }

    HashMap<CompressedDfsCode, HashSet<DfsEmbedding>>
      compressedCodeEmbeddings = new HashMap<>();

    for(ArrayList<DfsCode> dfsCodes : coverageDfsCodes.values()) {
      DfsCode minDfsCode = null;

      if(dfsCodes.size() > 1) {
        for(DfsCode dfsCode : dfsCodes) {
          if(minDfsCode == null ||
            dfsCodeComparator.compare(dfsCode, minDfsCode) < 0) {
            minDfsCode = dfsCode;
          }
        }
      }
      else {
        minDfsCode = dfsCodes.get(0);
      }

      CompressedDfsCode minCompressedDfsCode = new CompressedDfsCode
        (minDfsCode);

      HashSet<DfsEmbedding> minDfsCodeEmbeddings =
        compressedCodeEmbeddings.get(minCompressedDfsCode);

      HashSet<DfsEmbedding> coverageMinDfsCodeEmbeddings = codeEmbeddings
        .get(minDfsCode);

      if(minDfsCodeEmbeddings == null) {
        compressedCodeEmbeddings.put(minCompressedDfsCode, coverageMinDfsCodeEmbeddings);
      } else {
        minDfsCodeEmbeddings.addAll(coverageMinDfsCodeEmbeddings);
      }
    }
//
//    System.out.println("\n");
//
//    for(Map.Entry<CompressedDfsCode, HashSet<DfsEmbedding>> entry :
//      compressedCodeEmbeddings.entrySet()) {
//
//      System.out.println(entry.getKey() + " => " + entry.getValue().size());
//    }

    graph.setCodeEmbeddings(compressedCodeEmbeddings);
    graph.setActive(! compressedCodeEmbeddings.isEmpty());

    return graph;
  }
}
