package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.comparators.DfsCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.comparators.EdgePatternComparator;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.pojos.EdgePattern;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by peet on 03.05.16.
 */
public class PatternGrower implements Serializable {

  /**
   * DFS code comparator
   */
  private final DfsCodeComparator dfsCodeComparator;
  /**
   * edge pattern comparator
   */
  private final EdgePatternComparator<Integer> edgePatternComparator;

  public PatternGrower(FSMConfig fsmConfig) {
    boolean directed = fsmConfig.isDirected();

    this.dfsCodeComparator = new DfsCodeComparator(directed);
    this.edgePatternComparator = new EdgePatternComparator<>(directed);
  }

  public HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> growEmbeddings(
    ArrayList<AdjacencyList> adjacencyLists,
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> parentCodeEmbeddings) {
    // min DFS code per subgraph (set of edge ids)
    Map<Integer, HashSet<DFSCode>> coverageDfsCodes = new HashMap<>();
    Map<DFSCode, HashSet<DFSEmbedding>> codeEmbeddings = new HashMap<>();

    // for each supported DFS code

    for (Map.Entry<CompressedDFSCode, HashSet<DFSEmbedding>> entry :
      parentCodeEmbeddings.entrySet()) {

      CompressedDFSCode compressedDfsCode = entry.getKey();
      HashSet<DFSEmbedding> parentEmbeddings = entry.getValue();
      DFSCode parentDfsCode = compressedDfsCode.getDfsCode();

      EdgePattern<Integer> minPattern = parentDfsCode.getMinEdgePattern();
      List<Integer> rightmostPath = parentDfsCode
        .getRightMostPathVertexTimes();

      // for each embedding
      for (DFSEmbedding parentEmbedding : parentEmbeddings) {

        // rightmost path is inverse, so first element is rightmost vertex
        Boolean rightMostVertex = true;
        ArrayList<Integer> vertexTimes = parentEmbedding.getVertexTimes();

        // for each time on rightmost path
        for (Integer fromVertexTime : rightmostPath) {

          // query fromVertex data

          AdjacencyList adjacencyList = adjacencyLists
            .get(vertexTimes.get(fromVertexTime));
          Integer fromVertexLabel = adjacencyList.getVertexLabel();

          // for each incident edge
          for (AdjacencyListEntry adjacencyListEntry :
            adjacencyList.getEntries()) {

            boolean outgoing = adjacencyListEntry.isOutgoing();
            Integer edgeLabel = adjacencyListEntry.getEdgeLabel();
            Integer toVertexLabel = adjacencyListEntry.getVertexLabel();

            EdgePattern<Integer> candidatePattern = new EdgePattern<Integer>(
              fromVertexLabel, outgoing, edgeLabel, toVertexLabel);

            // PRUNING : continue only if edge pattern is lexicographically
            // larger than first step of DFS code
            if (edgePatternComparator
              .compare(minPattern, candidatePattern) <= 0) {

              Integer edgeIndex = adjacencyListEntry.getEdgeId();

              // allow only edges not already contained
              if (!parentEmbedding.getEdgeTimes().contains(edgeIndex)) {

                // query toVertexData
                Integer toVertexId = adjacencyListEntry.getVertexId();
                Integer toVertexTime = vertexTimes.indexOf(toVertexId);
                boolean forward = toVertexTime < 0;

                // PRUNING : grow only forward
                // or backward from rightmost vertex
                if (forward || rightMostVertex) {

                  DFSEmbedding embedding = DFSEmbedding
                    .deepCopy(parentEmbedding);
                  DFSCode dfsCode = DFSCode
                    .<Integer>deepCopy(parentDfsCode);

                  // add new vertex to embedding for forward steps
                  if (forward) {
                    embedding.getVertexTimes().add(toVertexId);
                    toVertexTime = embedding.getVertexTimes().size() - 1;
                  }

                  dfsCode.getSteps().add(new DFSStep(
                    fromVertexTime,
                    fromVertexLabel,
                    outgoing,
                    edgeLabel,
                    toVertexTime,
                    toVertexLabel
                  ));

                  embedding.getEdgeTimes().add(edgeIndex);

                  // check if subgraph already discovered
                  HashCodeBuilder builder = new HashCodeBuilder();
                  Set<Integer> mappedEdgeIndices = Sets
                    .newHashSet(embedding.getEdgeTimes());

                  for (Integer mappedEdgeIndex : mappedEdgeIndices) {
                    builder.append(mappedEdgeIndex);
                  }
                  Integer coverage = builder.hashCode();

                  HashSet<DFSCode> dfsCodes =
                    coverageDfsCodes.get(coverage);

                  if (dfsCodes == null) {
                    coverageDfsCodes.put(
                      coverage, Sets.newHashSet(dfsCode));
                  } else {
                    dfsCodes.add(dfsCode);
                  }

                  HashSet<DFSEmbedding> embeddings =
                    codeEmbeddings.get(dfsCode);

                  if (embeddings == null) {
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

    Map<Integer, HashSet<DFSCode>> coverageDfsCodes,
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
