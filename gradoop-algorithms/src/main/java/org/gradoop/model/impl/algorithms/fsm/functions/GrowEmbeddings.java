/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.comparators.DfsCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.comparators.EdgePatternComparator;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.pojos.EdgePattern;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Core of gSpan implementation. Grows embeddings of Frequent DFS codes.
 */
public class GrowEmbeddings extends
  RichMapFunction<SearchSpaceItem, SearchSpaceItem>
  implements CrossFunction
  <SearchSpaceItem, Collection<CompressedDFSCode>, SearchSpaceItem> {

  /**
   * broadcasting dataset name
   */
  public static final String DS_NAME = "compressedDfsCodes";
  /**
   * DFS code comparator
   */
  private final DfsCodeComparator dfsCodeComparator;
  /**
   * edge pattern comparator
   */
  private final EdgePatternComparator<Integer> edgePatternComparator;
  /**
   * frequent DFS codes
   */
  private Collection<CompressedDFSCode> frequentDfsCodes;

  /**
   * constructor
   * @param fsmConfig configuration
   */
  public GrowEmbeddings(FSMConfig fsmConfig) {
    boolean directed = fsmConfig.isDirected();

    this.dfsCodeComparator = new DfsCodeComparator(directed);
    this.edgePatternComparator = new EdgePatternComparator<>(directed);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Collection<CompressedDFSCode>> broadcast = getRuntimeContext()
      .getBroadcastVariable(DS_NAME);

    if (broadcast.isEmpty()) {
      this.frequentDfsCodes = null;
    } else {
      this.frequentDfsCodes = broadcast.get(0);
    }
  }

  @Override
  public SearchSpaceItem cross(SearchSpaceItem searchSpaceItem,
    Collection<CompressedDFSCode> frequentDfsCodes) throws Exception {

    if (searchSpaceItem.isCollector()) {
      searchSpaceItem = updateCollector(searchSpaceItem, frequentDfsCodes);
    } else {
      searchSpaceItem = growFrequentDfsCodeEmbeddings(
        searchSpaceItem, frequentDfsCodes);
    }
    return searchSpaceItem;
  }

  @Override
  public SearchSpaceItem map(SearchSpaceItem searchSpaceItem) throws Exception {

//    System.out.println(searchSpaceItem.getGraphId() +
//      " was triggered to grow / collect");

    if (frequentDfsCodes != null) {
      if (searchSpaceItem.isCollector()) {
        searchSpaceItem = updateCollector(searchSpaceItem, frequentDfsCodes);
      } else {
        searchSpaceItem = growFrequentDfsCodeEmbeddings(
          searchSpaceItem, frequentDfsCodes);
      }
    }
    return searchSpaceItem;
  }

  /**
   * appends frequent DFS codes collected so far by new ones
   * @param collector collector search space item
   * @param newFrequentDfsCodes new frequent DFS codes
   * @return updated collector
   */
  private SearchSpaceItem updateCollector(SearchSpaceItem collector,
    Collection<CompressedDFSCode> newFrequentDfsCodes) {

    collector.getFrequentDfsCodes().addAll(newFrequentDfsCodes);

    return collector;
  }

  /**
   * grows all embeddings of frequent DFS codes in a graph
   * @param graph graph search space item
   * @param frequentDfsCodes frequent DFS codes
   * @return graph with grown embeddings
   */
  private SearchSpaceItem growFrequentDfsCodeEmbeddings(SearchSpaceItem graph,
    Collection<CompressedDFSCode> frequentDfsCodes) {

    // min DFS code per subgraph (set of edge ids)
    Map<Integer, HashSet<DFSCode>> coverageDfsCodes = new HashMap<>();
    Map<DFSCode, HashSet<DFSEmbedding>> codeEmbeddings = new HashMap<>();

    // for each supported DFS code
    for (Map.Entry<CompressedDFSCode, HashSet<DFSEmbedding>> entry :
      graph.getCodeEmbeddings().entrySet()) {

      CompressedDFSCode compressedDfsCode = entry.getKey();

      // PRUNING : grow only embeddings of frequent DFS codes
      if (frequentDfsCodes.contains(compressedDfsCode)) {

        DFSCode parentDfsCode = compressedDfsCode.getDfsCode();
        EdgePattern<Integer> minPattern = parentDfsCode.getMinEdgePattern();
        List<Integer> rightmostPath = parentDfsCode
          .getRightMostPathVertexTimes();

        // for each embedding
        for (DFSEmbedding parentEmbedding : entry.getValue()) {

          // rightmost path is inverse, so first element is rightmost vertex
          Boolean rightMostVertex = true;
          ArrayList<Integer> vertexTimes = parentEmbedding.getVertexTimes();

          // for each time on rightmost path
          for (Integer fromVertexTime : rightmostPath) {

            // query fromVertex data
            AdjacencyList adjacencyList = graph.getAdjacencyLists()
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

//                    System.out.println(
//                      graph.getGraphId() +
//                        " grew " + parentDfsCode +
//                        " to " + dfsCode
//                    );
                  }
                }
              }
            }
            rightMostVertex = false;
          }
        }
      }
    }

    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> compressedCodeEmbeddings =
      getMinDfsCodesAndEmbeddings(coverageDfsCodes, codeEmbeddings);

    graph.setCodeEmbeddings(compressedCodeEmbeddings);
    graph.setActive(! compressedCodeEmbeddings.isEmpty());

//    if (compressedCodeEmbeddings.isEmpty()) {
//      System.out.println(graph.getGraphId() + " grew nothing");
//    }

    return graph;
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
