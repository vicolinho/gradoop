package org.gradoop.model.impl.algorithms.fsm.baseline;

import com.google.common.collect.Sets;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.Coverage;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.DfsCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.EdgePatternComparator;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.EdgePattern;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class NaivePatternGrower implements Serializable {

  /**
   * DFS code comparator
   */
  private final DfsCodeComparator dfsCodeComparator;
  /**
   * edge pattern comparator
   */
  private final EdgePatternComparator<Integer> edgePatternComparator;

  public NaivePatternGrower(FSMConfig fsmConfig) {
    boolean directed = fsmConfig.isDirected();

    this.dfsCodeComparator = new DfsCodeComparator(directed);
    this.edgePatternComparator = new EdgePatternComparator<>(directed);
  }

  public HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> growEmbeddings(
    ArrayList<AdjacencyList> adjacencyLists,
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> parentCodeEmbeddings) {
    // min DFS code per subgraph (set of edge ids)
    Map<Coverage, HashSet<DFSCode>> coverageDfsCodes = new HashMap<>();
    Map<DFSCode, HashSet<DFSEmbedding>> codeEmbeddings = new HashMap<>();

    // for each supported DFS code
    for(Map.Entry<CompressedDFSCode, HashSet<DFSEmbedding>> pair :
      parentCodeEmbeddings.entrySet()) {

      DFSCode parentCode = pair.getKey().getDfsCode();

      // for each embedding
      for(DFSEmbedding parentEmbedding : pair.getValue()) {

        // for each mapped vertex

        int fromTime = 0;
        for(Integer fromId : parentEmbedding.getVertexTimes()) {
          AdjacencyList adjacencyList = adjacencyLists.get(fromId);
          Integer fromLabel = adjacencyList.getVertexLabel();

          // for each adjacent edge
          for(AdjacencyListEntry entry : adjacencyList.getEntries()) {

            Integer edgeId = entry.getEdgeId();
            boolean outgoing = entry.isOutgoing();
            Integer edgeLabel = entry.getEdgeLabel();
            Integer toLabel = entry.getVertexLabel();

            // if edge not contained in parent
            if(!parentEmbedding.getEdgeTimes().contains(edgeId)) {

              DFSEmbedding childEmbedding =
                DFSEmbedding.deepCopy(parentEmbedding);
              DFSCode childCode =
                DFSCode.deepCopy(parentCode);


              Integer toId = entry.getVertexId();
              Integer toTime = parentEmbedding
                .getVertexTimes().indexOf(toId);

              // if other vertex not contained in parent
              if(toTime < 0) {
                toTime = childEmbedding.getVertexTimes().size();
                childEmbedding.getVertexTimes().add(toId);
              }

              childCode.getSteps().add(new DFSStep(
                fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel));

              childEmbedding.getEdgeTimes().add(edgeId);

              Coverage coverage =
                Coverage.fromIdList(childEmbedding.getEdgeTimes());

              // store code and embedding
              HashSet<DFSCode> siblingCodes =
                coverageDfsCodes.get(coverage);

              HashSet<DFSEmbedding> otherEmbeddings =
                codeEmbeddings.get(childCode);

              if(siblingCodes == null) {
                coverageDfsCodes.put(coverage, Sets.newHashSet(childCode));
              } else {
                siblingCodes.add(childCode);
              }

              if(otherEmbeddings == null) {
                codeEmbeddings.put(childCode, Sets.newHashSet(childEmbedding));
              } else {
                otherEmbeddings.add(childEmbedding);
              }
            }
          }
          fromTime++;
        }
      }
    }

    return getMinDfsCodesAndEmbeddings(coverageDfsCodes, codeEmbeddings);
  }

  /**
   * determines all grown minimum DFS codes and their embeddings in a map
   * @param coverageDfsCodes map : coverage => DFS codes
   * @param codeEmbeddings map : DFS code => embeddings
   * @return map : minimum DFS code per coverage => embeddings
   */
  private HashMap<CompressedDFSCode, HashSet<DFSEmbedding>>
  getMinDfsCodesAndEmbeddings(
    Map<Coverage, HashSet<DFSCode>> coverageDfsCodes,
    Map<DFSCode, HashSet<DFSEmbedding>> codeEmbeddings) {

    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>>
      compressedCodeEmbeddings = new HashMap<>();

    for (HashSet<DFSCode> dfsCodes : coverageDfsCodes.values()) {
      DFSCode minDfsCode = null;

      if (dfsCodes.size() > 1) {
        for (DFSCode dfsCode : dfsCodes) {
          if (minDfsCode == null ||
            dfsCodeComparator.compare(dfsCode, minDfsCode) < 0) {
            minDfsCode = dfsCode;
          }
        }
      } else {
        minDfsCode = dfsCodes.iterator().next();
      }

      CompressedDFSCode minCompressedDfsCode =
        new CompressedDFSCode(minDfsCode);

      HashSet<DFSEmbedding> minDfsCodeEmbeddings =
        compressedCodeEmbeddings.get(minCompressedDfsCode);

      HashSet<DFSEmbedding> coverageMinDfsCodeEmbeddings = codeEmbeddings
        .get(minDfsCode);

      if (minDfsCodeEmbeddings == null) {
        compressedCodeEmbeddings.put(minCompressedDfsCode,
          coverageMinDfsCodeEmbeddings);
      } else {
        minDfsCodeEmbeddings.addAll(coverageMinDfsCodeEmbeddings);
      }
    }
    return compressedCodeEmbeddings;
  }
}
