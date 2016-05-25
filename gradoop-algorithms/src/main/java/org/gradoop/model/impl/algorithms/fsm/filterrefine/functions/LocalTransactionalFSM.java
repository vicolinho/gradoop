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
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LocalTransactionalFSM implements FlatMapFunction
  <Tuple2<Integer, Map<Integer, Transaction>>,
    Tuple3<CompressedDFSCode, Integer,  Boolean>> {

  private final FSMConfig fsmConfig;
  private int workerId;

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

      GSpan.growEmbeddings(graph, fsmConfig.isDirected());

      if(graph.getCodeEmbeddings().isEmpty()) {
        inactiveGraphs.add(graphId);
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

      Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
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
