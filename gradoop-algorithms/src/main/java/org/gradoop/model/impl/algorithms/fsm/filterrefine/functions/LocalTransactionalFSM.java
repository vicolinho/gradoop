package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common
  .AbstractTransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.PatternGrower;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalTransactionalFSM implements FlatMapFunction
  <Tuple2<Integer, Map<Integer, Transaction>>,
    Tuple3<CompressedDFSCode, Integer,  Boolean>> {

  private final FSMConfig fsmConfig;
  private int workerId;

  private final PatternGrower grower;
  /**
   * minimum support
   */
  private Integer minSupport;
  private int minSupportForReport;

  private float threshold;

  private Map<Integer, Transaction> graphs;
  private Set<Integer> activeGraphIds;

  private final ArrayList<CompressedDFSCode> likelyFrequentDfsCodes = Lists
    .newArrayList();


  public LocalTransactionalFSM(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
    this.grower = new PatternGrower(fsmConfig);
    this.threshold = fsmConfig.getThreshold();
  }

  @Override
  public void flatMap(Tuple2<Integer, Map<Integer, Transaction>> pair,
    Collector<Tuple3<CompressedDFSCode, Integer, Boolean>> collector
  ) throws Exception {

    this.workerId = pair.f0;
    this.graphs = pair.f1;
    this.activeGraphIds = Sets.newHashSet(graphs.keySet());

    int graphCount = graphs.size();

    this.minSupport = (int) (threshold * (float) graphCount);
    this.minSupportForReport = (int) (0.05 * (float) graphCount);

    mine();

    for(CompressedDFSCode compressedDFSCode : likelyFrequentDfsCodes)
    {
      collector.collect(new Tuple3<>(compressedDFSCode, workerId,
        compressedDFSCode.getSupport() >= minSupport));
    }
  }

  public void mine() {

    boolean first = true;

    ArrayList<CompressedDFSCode> currentFrequentDfsCodes;

    int edgeCount = 1;
    int maxEdgeCount = fsmConfig.getMaxEdgeCount();
    maxEdgeCount = maxEdgeCount > 0 ?
      maxEdgeCount : AbstractTransactionalFSMiner.MAX_EDGE_COUNT;

    while ((first || !activeGraphIds.isEmpty()) &&
      edgeCount < maxEdgeCount) {

      if (first) {
        first = false;
      }

      growFrequentEmbeddings();
      edgeCount++;

      Map<CompressedDFSCode, Integer> currentDfsCodesWithSupport =
        reportPatterns();

      currentFrequentDfsCodes =
        findCurrentFrequentPatterns(currentDfsCodesWithSupport);

      deleteInfrequentEmbeddings(currentFrequentDfsCodes);
    }
  }

  private void growFrequentEmbeddings() {
    Collection<Integer> inactiveGraphs = Lists.newArrayList();

    for(Integer graphId : activeGraphIds) {

      Transaction graph = graphs.get(graphId);

      ArrayList<AdjacencyList> adjacencyLists = graph.getAdjacencyLists();

      HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> parentEmbeddings =
        graph.getCodeEmbeddings();

      HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> childEmbeddings =
        grower.growEmbeddings(adjacencyLists, parentEmbeddings);

      if(childEmbeddings.isEmpty()) {
        inactiveGraphs.add(graphId);
      } else {
        graph.setCodeEmbeddings(childEmbeddings);
      }
    }

    drop(inactiveGraphs);
  }

  private Map<CompressedDFSCode, Integer> reportPatterns() {
    Map<CompressedDFSCode, Integer> currentDfsCodes = Maps.newHashMap();

    for(Integer graphId : activeGraphIds) {

      Transaction graph = graphs.get(graphId);

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
      CompressedDFSCode code = entry.getKey();
      code.setSupport(support);

      likelyFrequentDfsCodes.add(code);

      if(support >= minSupport) {
        currentLikelyFrequentDfsCodes.add(code);
      }
    }
    return currentLikelyFrequentDfsCodes;
  }

  private void deleteInfrequentEmbeddings(
    ArrayList<CompressedDFSCode> currentLikelyFrequentDfsCodes) {
    Collection<Integer> inactiveGraphs = Lists.newArrayList();

    for(Integer graphId : activeGraphIds) {

      Transaction graph = graphs.get(graphId);

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
        inactiveGraphs.add(graphId);
      }
    }
    drop(inactiveGraphs);
  }

  private void drop(Collection<Integer> inactiveGraphs) {
    for(Integer graphId : inactiveGraphs) {
      activeGraphIds.remove(graphId);
    }
  }

}
