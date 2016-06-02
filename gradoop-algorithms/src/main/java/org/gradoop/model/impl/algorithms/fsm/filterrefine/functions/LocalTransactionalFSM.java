package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.SerializedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.SubgraphMessage;

import java.util.Collection;
import java.util.Map;

public class LocalTransactionalFSM implements FlatMapFunction
  <Tuple2<Integer, Collection<GSpanTransaction>>, SubgraphMessage> {

  private final FSMConfig fsmConfig;

  public LocalTransactionalFSM(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(Tuple2<Integer, Collection<GSpanTransaction>> pair,
    Collector<SubgraphMessage> collector
  ) throws Exception {
    Collection<GSpanTransaction> transactions = pair.f1;

    int graphCount = transactions.size();
    int minSupport = (int) (fsmConfig.getThreshold() * (float) graphCount) - 1;
    int minLikelySupport =
      (int) (fsmConfig.getLikelinessThreshold() * (float) graphCount) - 1;

    Collection<ObjectWithCount<DfsCode>> allLocallyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<ObjectWithCount<DfsCode>> likelyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<ObjectWithCount<DfsCode>> currentFrequentSubgraphs = null;

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
            ObjectWithCount<DfsCode> supportable = new ObjectWithCount<>(code, support);
            currentFrequentSubgraphs.add(supportable);
            allLocallyFrequentSubgraphs.add(supportable);
          }
        } else if (support >= minLikelySupport) {
          if (GSpan.isMinimumDfsCode(code, fsmConfig)) {
            likelyFrequentSubgraphs.add(new ObjectWithCount<>(code, support));
          }
        }
      }

      for (GSpanTransaction transaction : transactions) {
        GSpan.growFrequentSubgraphs(
          transaction, unwrap(currentFrequentSubgraphs), fsmConfig);
      }

      edgeCount++;
    } while (! currentFrequentSubgraphs.isEmpty()
      && edgeCount <= fsmConfig.getMaxEdgeCount());

    collect(collector, pair.f0,
      allLocallyFrequentSubgraphs, likelyFrequentSubgraphs);
  }

  private Collection<DfsCode> unwrap(Collection<ObjectWithCount<DfsCode>> wrappedCodes) {

    Collection<DfsCode> codes = Lists.newArrayListWithExpectedSize(wrappedCodes.size());


    for (ObjectWithCount<DfsCode> wrappedCode : wrappedCodes) {
      codes.add(wrappedCode.getObject());
    }

    return codes;
  }

  private Map<DfsCode, Integer> countSupport(
    Collection<GSpanTransaction> transactions) {

    Map<DfsCode, Integer> codeSupport = Maps.newHashMap();

    for (GSpanTransaction transaction : transactions) {
      for (DfsCode code : transaction.getCodeEmbeddings().keySet()) {

        Integer support = codeSupport.get(code);
        support = support == null ? 1 : support + 1;

        codeSupport.put(code, support);
      }
    }

    return codeSupport;
  }


  private void collect(
    Collector<SubgraphMessage> collector,
    int workerId, Collection<ObjectWithCount<DfsCode>> locallyFrequentDfsCodes,
    Collection<ObjectWithCount<DfsCode>> likelyFrequentDfsCodes) {
    for(ObjectWithCount<DfsCode> subgraph : locallyFrequentDfsCodes)
    {
      collector.collect(new SubgraphMessage(
        new CompressedSubgraph(subgraph.getObject()),
        subgraph.getSupport(), workerId, true));
    }
    for(ObjectWithCount<DfsCode> subgraph : likelyFrequentDfsCodes)
    {
      collector.collect(new SubgraphMessage(
        new CompressedSubgraph(subgraph.getObject()),
        subgraph.getSupport(), workerId, false));
    }
  }
}
