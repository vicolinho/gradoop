package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples.FilterResult;

import java.util.Collection;
import java.util.Map;

public class LocalGSpan implements FlatMapFunction
  <Tuple2<Integer, Collection<GSpanTransaction>>, FilterResult> {

  private final FSMConfig fsmConfig;

  public LocalGSpan(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(Tuple2<Integer, Collection<GSpanTransaction>> pair,
    Collector<FilterResult> collector
  ) throws Exception {
    Collection<GSpanTransaction> transactions = pair.f1;

    int graphCount = transactions.size();
    int minSupport = (int) (fsmConfig.getThreshold() * (float) graphCount) - 1;
    int minLikelySupport =
      (int) (fsmConfig.getLikelinessThreshold() * (float) graphCount) - 1;

    Collection<WithCount<DfsCode>> allLocallyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<WithCount<DfsCode>> likelyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<WithCount<DfsCode>> currentFrequentSubgraphs = null;

    int edgeCount = 1;
    do {
      // count support
      Map<DfsCode, Integer> codeSupport = countSupport(transactions);

      currentFrequentSubgraphs = Lists.newArrayList();

      for (Map.Entry<DfsCode, Integer> entry : codeSupport.entrySet())
      {
        DfsCode code = entry.getKey();
        int support = entry.getValue();

        if (support >= minSupport) {
          if(GSpan.isMinimumDfsCode(code, fsmConfig)) {
            WithCount<DfsCode> supportable = new WithCount<>(code, support);
            currentFrequentSubgraphs.add(supportable);
            allLocallyFrequentSubgraphs.add(supportable);
          }
        } else if (support >= minLikelySupport) {
          if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
            likelyFrequentSubgraphs.add(new WithCount<>(code, support));
          }
        }
      }

      for (GSpanTransaction transaction : transactions) {
        if (transaction.hasGrownSubgraphs()) {
          GSpan.growEmbeddings(
            transaction, unwrap(currentFrequentSubgraphs), fsmConfig);
        }
      }

      edgeCount++;
    } while (! currentFrequentSubgraphs.isEmpty()
      && edgeCount <= fsmConfig.getMaxEdgeCount());

    collect(collector, pair.f0,
      allLocallyFrequentSubgraphs, likelyFrequentSubgraphs);
  }

  private Collection<DfsCode> unwrap(Collection<WithCount<DfsCode>> wrappedCodes) {

    Collection<DfsCode> codes = Lists.newArrayListWithExpectedSize(wrappedCodes.size());

    for (WithCount<DfsCode> wrappedCode : wrappedCodes) {
      codes.add(wrappedCode.getObject());
    }

    return codes;
  }

  private Map<DfsCode, Integer> countSupport(
    Collection<GSpanTransaction> transactions) {

    Map<DfsCode, Integer> codeSupport = Maps.newHashMap();

    for (GSpanTransaction transaction : transactions) {
      if (transaction.hasGrownSubgraphs()) {
        for (DfsCode code : transaction.getCodeEmbeddings().keySet()) {

          Integer support = codeSupport.get(code);
          support = support == null ? 1 : support + 1;

          codeSupport.put(code, support);
        }
      }
    }

    return codeSupport;
  }


  private void collect(
    Collector<FilterResult> collector,
    int workerId, Collection<WithCount<DfsCode>> locallyFrequentDfsCodes,
    Collection<WithCount<DfsCode>> likelyFrequentDfsCodes) {
    for(WithCount<DfsCode> subgraph : locallyFrequentDfsCodes)
    {
      collector.collect(new FilterResult(
        new CompressedSubgraph(subgraph.getObject()),
        subgraph.getCount(), workerId, true));
    }
    for(WithCount<DfsCode> subgraph : likelyFrequentDfsCodes)
    {
      collector.collect(new FilterResult(
        new CompressedSubgraph(subgraph.getObject()),
        subgraph.getCount(), workerId, false));
    }
  }
}
