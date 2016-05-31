package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanTransaction;

import java.util.Collection;
import java.util.Map;

public class LocalTransactionalFSM implements FlatMapFunction
  <Tuple2<Integer, Collection<GSpanTransaction>>,
    Tuple3<CompressedDfsCode, Integer,  Boolean>> {

  private final FSMConfig fsmConfig;

  public LocalTransactionalFSM(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(Tuple2<Integer, Collection<GSpanTransaction>> pair,
    Collector<Tuple3<CompressedDfsCode, Integer, Boolean>> collector
  ) throws Exception {
    Collection<GSpanTransaction> transactions = pair.f1;

    int graphCount = transactions.size();
    int minSupport = (int) (fsmConfig.getThreshold() * (float) graphCount) - 1;
    int minLikelySupport =
      (int) (fsmConfig.getLikelinessThreshold() * (float) graphCount) - 1;

    Collection<CompressedDfsCode> allLocallyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<CompressedDfsCode> likelyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<CompressedDfsCode> currentFrequentSubgraphs = null;

    int edgeCount = 1;
    do {
      // count support
      Map<CompressedDfsCode, Integer> codeSupport = countSupport(transactions);

      currentFrequentSubgraphs = Lists.newArrayList();

      for (Map.Entry<CompressedDfsCode, Integer> entry : codeSupport.entrySet())
      {
        CompressedDfsCode code = entry.getKey();
        int support = entry.getValue();

        if (support >= minSupport) {
          if(GSpan.isMinimumDfsCode(code, fsmConfig)) {
            code.setSupport(support);
            currentFrequentSubgraphs.add(code);
            allLocallyFrequentSubgraphs.add(code);
          }
        } else if (support >= minLikelySupport) {
          if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
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

  private Map<CompressedDfsCode, Integer> countSupport(
    Collection<GSpanTransaction> transactions) {

    Map<CompressedDfsCode, Integer> codeSupport = Maps.newHashMap();

    for (GSpanTransaction transaction : transactions) {
      for (CompressedDfsCode code : transaction.getCodeEmbeddings().keySet()) {

        Integer support = codeSupport.get(code);
        support = support == null ? 1 : support + 1;

        codeSupport.put(code, support);
      }
    }

    return codeSupport;
  }


  private void collect(
    Collector<Tuple3<CompressedDfsCode, Integer, Boolean>> collector,
    int workerId, Collection<CompressedDfsCode> locallyFrequentDfsCodes,
    Collection<CompressedDfsCode> likelyFrequentDfsCodes) {
    for(CompressedDfsCode subgraph : locallyFrequentDfsCodes)
    {
      collector.collect(new Tuple3<>(subgraph, workerId, true));
    }
    for(CompressedDfsCode subgraph : likelyFrequentDfsCodes)
    {
      collector.collect(new Tuple3<>(subgraph, workerId, false));
    }
  }
}
