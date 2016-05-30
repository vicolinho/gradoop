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

    int edgeCount = 1;
    do {
      // count support
      Map<CompressedDFSCode, Integer> codeSupport = countSupport(transactions);

      currentFrequentSubgraphs = Lists.newArrayList();

      for (Map.Entry<CompressedDFSCode, Integer> entry : codeSupport.entrySet())
      {
        CompressedDFSCode code = entry.getKey();
        int support = entry.getValue();

        if (support >= minSupport) {
          if(GSpan.isValidMinimumDfsCode(code, fsmConfig)) {
            code.setSupport(support);
            currentFrequentSubgraphs.add(code);
            allLocallyFrequentSubgraphs.add(code);
          }
        } else if (support >= minLikelySupport) {
          if (GSpan.isValidMinimumDfsCode(code, fsmConfig)) {
            code.setSupport(support);
            likelyFrequentSubgraphs.add(code);
          }
        }
      }

      for (GSpanTransaction transaction : transactions) {
        GSpan.growFrequentSubgraphs(
          transaction, currentFrequentSubgraphs, fsmConfig);
      }

      edgeCount++;
    } while (! currentFrequentSubgraphs.isEmpty()
      && edgeCount <= fsmConfig.getMaxEdgeCount());

    collect(collector, pair.f0,
      allLocallyFrequentSubgraphs, likelyFrequentSubgraphs);
  }

  private Map<CompressedDFSCode, Integer> countSupport(
    Collection<GSpanTransaction> transactions) {

    Map<CompressedDFSCode, Integer> codeSupport = Maps.newHashMap();

    for (GSpanTransaction transaction : transactions) {
      for (CompressedDFSCode code : transaction.getCodeEmbeddings().keySet()) {

        Integer support = codeSupport.get(code);
        support = support == null ? 1 : support + 1;

        codeSupport.put(code, support);
      }
    }

    return codeSupport;
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
