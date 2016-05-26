package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;

import java.util.Collection;
import java.util.Map;

public class LocalTransactionalFSM implements FlatMapFunction
  <Tuple2<Integer, Collection<GSpanTransaction>>,
    Tuple3<CompressedDFSCode, Integer,  Boolean>> {

  private final FSMConfig fsmConfig;

  public LocalTransactionalFSM(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(Tuple2<Integer, Collection<GSpanTransaction>> pair,
    Collector<Tuple3<CompressedDFSCode, Integer, Boolean>> collector
  ) throws Exception {
    Collection<GSpanTransaction> transactions = pair.f1;

    int graphCount = transactions.size();
    int minSupport = (int) (fsmConfig.getThreshold() * (float) graphCount) - 1;
    int minLikelySupport =
      (int) (fsmConfig.getLikelinessThreshold() * (float) graphCount) - 1;

    Collection<CompressedDFSCode> allLocallyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<CompressedDFSCode> likelyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<CompressedDFSCode> currentFrequentSubgraphs = null;

    int edgeCount = 2;
    do {
      // grow and report frequent subgraphs
      Collection<CompressedDFSCode> reportedSubgraphs =
        growFrequentSubgraphs(transactions, currentFrequentSubgraphs);

      // reset
      currentFrequentSubgraphs = Lists.newArrayList();

      // count support
      Map<CompressedDFSCode, Integer> codeSupport =
        countSupport(reportedSubgraphs);

      // determine valid (likely) frequent subgraphs
      postPruneSubgraphs(codeSupport,
        minSupport, currentFrequentSubgraphs,
        minLikelySupport, likelyFrequentSubgraphs);

      allLocallyFrequentSubgraphs.addAll(currentFrequentSubgraphs);
      edgeCount++;
    } while (! currentFrequentSubgraphs.isEmpty()
      && edgeCount <= fsmConfig.getMaxEdgeCount());

    collect(collector, pair.f0,
      allLocallyFrequentSubgraphs, likelyFrequentSubgraphs);
  }

  private Collection<CompressedDFSCode> growFrequentSubgraphs(
    Collection<GSpanTransaction> transactions,
    Collection<CompressedDFSCode> frequentSubgraphs) {
    Collection<CompressedDFSCode> reportedSubgraphs = Lists.newArrayList();

    for (GSpanTransaction transaction : transactions) {
      // if transaction could grow last iteration
      if(frequentSubgraphs == null || transaction.hasGrownSubgraphs()) {

        // grow
        GSpan.growFrequentSubgraphs(
          transaction, frequentSubgraphs, fsmConfig);

        // report
        if(transaction.hasGrownSubgraphs()) {
          reportedSubgraphs.addAll(transaction.getCodeEmbeddings().keySet());
        }
      }
    }
    return reportedSubgraphs;
  }

  private Map<CompressedDFSCode, Integer> countSupport(
    Collection<CompressedDFSCode> reportedSubgraphs) {
    Map<CompressedDFSCode, Integer> codeSupport = Maps.newHashMap();

    for(CompressedDFSCode code : reportedSubgraphs) {

      Integer support = codeSupport.get(code);
      support = support == null ? 1 : support + 1;

      codeSupport.put(code, support);
    }
    return codeSupport;
  }

  private void postPruneSubgraphs(
    Map<CompressedDFSCode, Integer> codeSupport, int minSupport,
    Collection<CompressedDFSCode> currentFrequentSubgraphs,
    int minLikelySupport,
    Collection<CompressedDFSCode> likelyFrequentSubgraphs) {
    for (
      Map.Entry<CompressedDFSCode, Integer> entry : codeSupport.entrySet())
    {
      Integer support = entry.getValue();
      CompressedDFSCode code = entry.getKey();

      if (support >= minSupport &&
        GSpan.isValidMinimumDfsCode(code, fsmConfig)) {
        code.setSupport(support);
        currentFrequentSubgraphs.add(code);
      } else if (support >= minLikelySupport &&
        GSpan.isValidMinimumDfsCode(code, fsmConfig)) {
        code.setSupport(support);
        likelyFrequentSubgraphs.add(code);
      }
    }
  }

  private void collect(
    Collector<Tuple3<CompressedDFSCode, Integer, Boolean>> collector,
    int workerId, Collection<CompressedDFSCode> locallyFrequentDfsCodes,
    Collection<CompressedDFSCode> likelyFrequentDfsCodes) {
    for(CompressedDFSCode compressedDFSCode : locallyFrequentDfsCodes)
    {
      collector.collect(new Tuple3<>(compressedDFSCode, workerId, true));
    }
    for(CompressedDFSCode compressedDFSCode : likelyFrequentDfsCodes)
    {
      collector.collect(new Tuple3<>(compressedDFSCode, workerId, false));
    }
  }
}
