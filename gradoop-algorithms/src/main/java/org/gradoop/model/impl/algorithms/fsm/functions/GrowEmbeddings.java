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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.operators.tostring.comparators.DfsCodeComparator;
import org.gradoop.model.impl.operators.tostring.comparators.EdgePatternComparator;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.operators.tostring.pojos.DFSCode;
import org.gradoop.model.impl.operators.tostring.pojos.DFSEmbedding;
import org.gradoop.model.impl.operators.tostring.pojos.DFSStep;
import org.gradoop.model.impl.operators.tostring.pojos.EdgePattern;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Core of gSpan implementation. Grows embeddings of Frequent DFS codes.
 */
public class GrowEmbeddings<L extends Comparable<L>> extends 
  RichMapFunction<SearchSpaceItem<L>, SearchSpaceItem<L>>
  implements CrossFunction
  <SearchSpaceItem<L>, Collection<CompressedDFSCode<L>>, SearchSpaceItem<L>> {

  public static String DS_NAME = "compressedDfsCodes";
  /**
   * DFS code comparator
   */
  private final DfsCodeComparator dfsCodeComparator;
  /**
   * edge pattern comparator
   */
  private final EdgePatternComparator<L> edgePatternComparator;
  private Collection<CompressedDFSCode<L>> frequentDfsCodes;

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
    List<Collection<CompressedDFSCode<L>>> broadcast = getRuntimeContext()
      .getBroadcastVariable(DS_NAME);

    if(broadcast.isEmpty()) {
      this.frequentDfsCodes = null;
    } else {
      this.frequentDfsCodes = broadcast.get(0);
    }
  }

  @Override
  public SearchSpaceItem<L> cross(SearchSpaceItem<L> searchSpaceItem,
    Collection<CompressedDFSCode<L>> frequentDfsCodes) throws Exception {

    if (searchSpaceItem.isCollector()) {
      searchSpaceItem = updateCollector(searchSpaceItem, frequentDfsCodes);
    } else {
      searchSpaceItem = growFrequentDfsCodeEmbeddings(
        searchSpaceItem, frequentDfsCodes);
    }
    return searchSpaceItem;
  }

  @Override
  public SearchSpaceItem<L> map(SearchSpaceItem<L> searchSpaceItem) throws Exception {

//    System.out.println(searchSpaceItem.getGraphId() +
//      " was triggered to grow / collect");

    if(frequentDfsCodes != null) {
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
  private SearchSpaceItem<L> updateCollector(SearchSpaceItem<L> collector,
    Collection<CompressedDFSCode<L>> newFrequentDfsCodes) {

    collector.getFrequentDfsCodes().addAll(newFrequentDfsCodes);

    return collector;
  }

  /**
   * grows all embeddings of frequent DFS codes in a graph
   * @param graph graph search space item
   * @param frequentDfsCodes frequent DFS codes
   * @return graph with grown embeddings
   */
  private SearchSpaceItem<L> growFrequentDfsCodeEmbeddings(SearchSpaceItem<L> graph,
    Collection<CompressedDFSCode<L>> frequentDfsCodes) {

    // min DFS code per subgraph (set of edge ids)
    Map<Integer, HashSet<DFSCode<L>>> coverageDfsCodes = new HashMap<>();
    Map<DFSCode<L>, HashSet<DFSEmbedding>> codeEmbeddings = new HashMap<>();

    // for each supported DFS code
    for (Map.Entry<CompressedDFSCode<L>, HashSet<DFSEmbedding>> entry :
      graph.getCodeEmbeddings().entrySet()) {

      CompressedDFSCode<L> compressedDfsCode = entry.getKey();

      // PRUNING : grow only embeddings of frequent DFS codes
      if (frequentDfsCodes.contains(compressedDfsCode)) {

        DFSCode<L> parentDfsCode = compressedDfsCode.getDfsCode();
        EdgePattern<L> minPattern = parentDfsCode.getMinEdgePattern();
        List<Integer> rightmostPath = parentDfsCode
          .getRightMostPathVertexTimes();

        // for each embedding
        for (DFSEmbedding parentEmbedding : entry.getValue()) {

          // rightmost path is inverse, so first element is rightmost vertex
          Boolean rightMostVertex = true;
          ArrayList<GradoopId> vertexTimes = parentEmbedding.getVertexTimes();

          // for each time on rightmost path
          for (Integer fromVertexTime : rightmostPath) {

            // query fromVertex data
            AdjacencyList<L> adjacencyList = graph.getAdjacencyLists()
              .get(vertexTimes.get(fromVertexTime));
            L fromVertexLabel = adjacencyList.getVertexLabel();

            // for each incident edge
            for (AdjacencyListEntry<L> adjacencyListEntry :
              adjacencyList.getEntries()) {

              boolean outgoing = adjacencyListEntry.isOutgoing();
              L edgeLabel = adjacencyListEntry.getEdgeLabel();
              L toVertexLabel = adjacencyListEntry.getVertexLabel();

              EdgePattern<L> candidatePattern = new EdgePattern<L>(
                fromVertexLabel, outgoing, edgeLabel, toVertexLabel);

              // PRUNING : continue only if edge pattern is lexicographically
              // larger than first step of DFS code
              if (edgePatternComparator
                .compare(minPattern, candidatePattern) <= 0) {

                GradoopId edgeId = adjacencyListEntry.getEdgeId();

                // allow only edges not already contained
                if (!parentEmbedding.getEdgeTimes().contains(edgeId)) {

                  // query toVertexData
                  GradoopId toVertexId = adjacencyListEntry.getVertexId();
                  Integer toVertexTime = vertexTimes.indexOf(toVertexId);
                  boolean forward = toVertexTime < 0;

                  // PRUNING : grow only forward
                  // or backward from rightmost vertex
                  if (forward || rightMostVertex) {

                    DFSEmbedding embedding = DFSEmbedding
                      .deepCopy(parentEmbedding);
                    DFSCode<L> dfsCode = DFSCode
                      .<L>deepCopy(parentDfsCode);

                    // add new vertex to embedding for forward steps
                    if (forward) {
                      embedding.getVertexTimes().add(toVertexId);
                      toVertexTime = embedding.getVertexTimes().size() - 1;
                    }

                    dfsCode.getSteps().add(new DFSStep<L>(
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

                    HashSet<DFSCode<L>> dfsCodes =
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

    HashMap<CompressedDFSCode<L>, HashSet<DFSEmbedding>> compressedCodeEmbeddings =
      getMinDfsCodesAndEmbeddings(coverageDfsCodes, codeEmbeddings);

    graph.setCodeEmbeddings(compressedCodeEmbeddings);
    graph.setActive(! compressedCodeEmbeddings.isEmpty());

//    if(compressedCodeEmbeddings.isEmpty()) {
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
  private HashMap<CompressedDFSCode<L>, HashSet<DFSEmbedding>>
  getMinDfsCodesAndEmbeddings(

    Map<Integer, HashSet<DFSCode<L>>> coverageDfsCodes,
    Map<DFSCode<L>, HashSet<DFSEmbedding>> codeEmbeddings) {
    HashMap<CompressedDFSCode<L>, HashSet<DFSEmbedding>>
      compressedCodeEmbeddings = new HashMap<>();

    for (HashSet<DFSCode<L>> dfsCodes : coverageDfsCodes.values()) {
      DFSCode<L> minDfsCode = null;

      if (dfsCodes.size() > 1) {
        for (DFSCode<L> dfsCode : dfsCodes) {
          if (minDfsCode == null ||
            dfsCodeComparator.compare(dfsCode, minDfsCode) < 0) {
            minDfsCode = dfsCode;
          }
        }
      } else {
        minDfsCode = dfsCodes.iterator().next();
      }

      CompressedDFSCode<L> minCompressedDfsCode =
        new CompressedDFSCode<L>(minDfsCode);

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
