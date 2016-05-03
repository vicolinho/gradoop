package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples
  .FilterAndRefineSearchSpaceItem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalFSM extends RichGroupCombineFunction
  <FilterAndRefineSearchSpaceItem,  CompressedDFSCode> {

  /**
   * name of broadcast data set for minimum support
   */
  public static final String DS_NAME = "minSupport";
  private final PatternGrower grower;
  /**
   * minimum support
   */
  private Integer minSupport;

  private final Map<Integer, FilterAndRefineSearchSpaceItem> graphs = Maps
    .newHashMap();

  /**
   * constructor
   * @param fsmConfig configuration
   */
  public LocalFSM(FSMConfig fsmConfig) {
    this.grower = new PatternGrower(fsmConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minSupport = getRuntimeContext()
      .<Integer>getBroadcastVariable(DS_NAME)
      .get(0);
  }

  @Override
  public void combine(Iterable<FilterAndRefineSearchSpaceItem> iterable,
    Collector<CompressedDFSCode> collector) throws Exception {

    indexGraphs(iterable);

    boolean first = true;

    ArrayList<CompressedDFSCode> allLikelyFrequentDfsCodes = Lists
      .newArrayList();
    ArrayList<CompressedDFSCode> currentLikelyFrequentDfsCodes = null;

    while (first || !graphs.isEmpty()) {
      if (first) {
        first = false;
      }

      growFrequentEmbeddings();

      Map<CompressedDFSCode, Integer> currentDfsCodes = reportDfsCodeSupport();

      currentLikelyFrequentDfsCodes =
        findCurrentFrequentDfsCodes(currentDfsCodes);

      deleteInfrequentEmbedding(currentLikelyFrequentDfsCodes);

      allLikelyFrequentDfsCodes.addAll(currentLikelyFrequentDfsCodes);
    }


    for(CompressedDFSCode compressedDFSCode : allLikelyFrequentDfsCodes) {
      collector.collect(compressedDFSCode);
    }
  }

  private void deleteInfrequentEmbedding(
    ArrayList<CompressedDFSCode> currentLikelyFrequentDfsCodes) {
    Collection<Integer> inactiveGraphs = Lists.newArrayList();

    for(Map.Entry<Integer, FilterAndRefineSearchSpaceItem> entry : graphs
      .entrySet()) {

      FilterAndRefineSearchSpaceItem graph = entry.getValue();

      HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddings =
        graph.getCodeEmbeddings();

      Set<CompressedDFSCode> supportedCodes = Sets
        .newHashSet(codeEmbeddings.keySet());

      for(CompressedDFSCode supportedCode : supportedCodes) {
        if(! currentLikelyFrequentDfsCodes.contains(supportedCode)) {
          codeEmbeddings.remove(supportedCode);
        }
      }

      if(codeEmbeddings.isEmpty()) {
        inactiveGraphs.add(entry.getKey());
      }
    }
    drop(inactiveGraphs);
  }

  private ArrayList<CompressedDFSCode> findCurrentFrequentDfsCodes(
    Map<CompressedDFSCode, Integer> currentDfsCodes) {
    ArrayList<CompressedDFSCode> currentLikelyFrequentDfsCodes;
    currentLikelyFrequentDfsCodes = Lists.newArrayList();

    for(Map.Entry<CompressedDFSCode, Integer> entry : currentDfsCodes
      .entrySet()) {

      Integer support = entry.getValue();

      if(support >= minSupport) {
        CompressedDFSCode code = entry.getKey();
        code.setSupport(support);
        currentLikelyFrequentDfsCodes.add(code);
      }
    }
    return currentLikelyFrequentDfsCodes;
  }

  private Map<CompressedDFSCode, Integer> reportDfsCodeSupport() {
    Map<CompressedDFSCode, Integer> currentDfsCodes = Maps.newHashMap();

    for(FilterAndRefineSearchSpaceItem graph : graphs.values()) {
      for(CompressedDFSCode code : graph.getCodeEmbeddings().keySet()) {
        Integer support = currentDfsCodes.get(code);
        currentDfsCodes.put(code, (support == null) ? 1 : support + 1);
      }
    }
    return currentDfsCodes;
  }

  private void growFrequentEmbeddings() {
    Collection<Integer> inactiveGraphs = Lists.newArrayList();

    for(Map.Entry<Integer, FilterAndRefineSearchSpaceItem> entry : graphs
      .entrySet()) {

      FilterAndRefineSearchSpaceItem graph = entry.getValue();

      ArrayList<AdjacencyList> adjacencyLists = graph.getAdjacencyLists();

      HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> parentEmbeddings =
        graph.getCodeEmbeddings();

      HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> childEmbeddings =
        grower.growEmbeddings(adjacencyLists, parentEmbeddings);

      if(childEmbeddings.isEmpty()) {
        inactiveGraphs.add(entry.getKey());
      } else {
        graph.setCodeEmbeddings(childEmbeddings);
      }
    }

    drop(inactiveGraphs);
  }

  private void drop(Collection<Integer> inactiveGraphs) {
    for(Integer graphId : inactiveGraphs) {
      graphs.remove(graphId);
    }
  }

  private void indexGraphs(Iterable<FilterAndRefineSearchSpaceItem> iterable) {
    int graphId = 0;
    for(FilterAndRefineSearchSpaceItem graph : iterable) {

      graphs.put(graphId, graph);

      graphId++;
    }
  }
}
