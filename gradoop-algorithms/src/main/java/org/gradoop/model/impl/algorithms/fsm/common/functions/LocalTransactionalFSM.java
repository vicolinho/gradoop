package org.gradoop.model.impl.algorithms.fsm.common.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.PatternGrower;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.naiveparallel.pojos.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalTransactionalFSM extends RichGroupCombineFunction
  <Transaction,  CompressedDFSCode> {

  private final PatternGrower grower;
  /**
   * minimum support
   */
  private Integer minSupport;

  private final Map<Integer, Transaction> graphs = Maps
    .newHashMap();

  /**
   * constructor
   * @param fsmConfig configuration
   */
  public LocalTransactionalFSM(FSMConfig fsmConfig) {
    this.grower = new PatternGrower(fsmConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minSupport = getRuntimeContext()
      .<Integer>getBroadcastVariable(BroadcastNames.MIN_SUPPORT)
      .get(0);
  }

  @Override
  public void combine(Iterable<Transaction> iterable,
    Collector<CompressedDFSCode> collector) throws Exception {

    indexGraphs(iterable);

    boolean first = true;

    ArrayList<CompressedDFSCode> allLikelyFrequentDfsCodes = Lists
      .newArrayList();
    ArrayList<CompressedDFSCode> currentLikelyFrequentDfsCodes;

    while (first || !graphs.isEmpty()) {
      if (first) {
        first = false;
      }

      growFrequentEmbeddings();

      Map<CompressedDFSCode, Integer> currentDfsCodes = reportPatterns();

      currentLikelyFrequentDfsCodes =
        findCurrentFrequentPatterns(currentDfsCodes);

      deleteInfrequentEmbeddings(currentLikelyFrequentDfsCodes);

      allLikelyFrequentDfsCodes.addAll(currentLikelyFrequentDfsCodes);
    }

    for(CompressedDFSCode compressedDFSCode : allLikelyFrequentDfsCodes) {
      collector.collect(compressedDFSCode);
    }
  }

  private void indexGraphs(Iterable<Transaction> iterable) {
    int graphId = 0;
    for(Transaction graph : iterable) {

      graphs.put(graphId, graph);

      graphId++;
    }
  }

  private void growFrequentEmbeddings() {
    Collection<Integer> inactiveGraphs = Lists.newArrayList();

    for(Map.Entry<Integer, Transaction> entry : graphs
      .entrySet()) {

      Transaction graph = entry.getValue();

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

  private Map<CompressedDFSCode, Integer> reportPatterns() {
    Map<CompressedDFSCode, Integer> currentDfsCodes = Maps.newHashMap();

    for(Transaction graph : graphs.values()) {
      for(CompressedDFSCode code : graph.getCodeEmbeddings().keySet()) {
        Integer support = currentDfsCodes.get(code);
        currentDfsCodes.put(code, (support == null) ? 1 : support + 1);
      }
    }
    return currentDfsCodes;
  }

  private ArrayList<CompressedDFSCode> findCurrentFrequentPatterns(
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

  private void deleteInfrequentEmbeddings(
    ArrayList<CompressedDFSCode> currentLikelyFrequentDfsCodes) {
    Collection<Integer> inactiveGraphs = Lists.newArrayList();

    for(Map.Entry<Integer, Transaction> entry : graphs
      .entrySet()) {

      Transaction graph = entry.getValue();

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

  private void drop(Collection<Integer> inactiveGraphs) {
    for(Integer graphId : inactiveGraphs) {
      graphs.remove(graphId);
    }
  }
}
