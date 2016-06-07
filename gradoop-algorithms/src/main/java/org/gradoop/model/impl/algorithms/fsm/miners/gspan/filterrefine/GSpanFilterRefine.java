package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.GSpanBase;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions.*;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples.FilterResult;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples
  .RefinementMessage;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;

import java.util.Collection;
import java.util.Map;


public class GSpanFilterRefine
  extends GSpanBase {


  @Override
  public DataSet<WithCount<CompressedSubgraph>> mine(DataSet<EdgeTriple> edges,
                                                      DataSet<Integer> minSupport, FsmConfig fsmConfig) {

    setFsmConfig(fsmConfig);
    DataSet<GSpanTransaction> transactions = createTransactions(edges);

    // distribute graphs to workers
    DataSet<Tuple2<Integer, Collection<GSpanTransaction>>> partitions =
      transactions
        .rebalance()
        .mapPartition(new SearchSpacePartition());

    // get worker ids with local graph counts
    DataSet<Map<Integer, Integer>> workerIdsGraphCount = partitions
      .map(new WorkerIdGraphCount())
      .reduceGroup(new WorkerIdsGraphCounts());

    // FILTER round
    DataSet<FilterResult> fsmResult =
      partitions
        // run local FSM
        .flatMap(new LocalGSpan(fsmConfig));

    DataSet<RefinementMessage> filterResult =
      fsmResult
        // group reports by DFS code
        .groupBy(0)
        // keep if sure or likely globally frequent; drop otherwise
        .reduceGroup(new FrequentOrRefinementCandidate(fsmConfig))
        .withBroadcastSet(minSupport, BroadcastNames.MIN_FREQUENCY)
        .withBroadcastSet(workerIdsGraphCount, BroadcastNames.WORKER_GRAPHCOUNT);

    // add globally frequent DFS codes to result
    DataSet<WithCount<CompressedSubgraph>> frequentDfsCodes = filterResult
      .filter(new CompleteResult())
      .map(new CompressedSubgraphWithCount());

    // REFINEMENT

    // remember incomplete results
    DataSet<WithCount<CompressedSubgraph>> partialResults = filterResult
      .filter(new PartialResult())
      .map(new CompressedSubgraphWithCount());

    // get refined results
    DataSet<WithCount<CompressedSubgraph>> refinementResults =
      filterResult
      .filter(new RefinementCall())
      .groupBy(1) // workerId
      .reduceGroup(new RefinementCalls())
      .join(partitions)
      .where(0).equalTo(0)
      .with(new Refinement(fsmConfig));

    frequentDfsCodes = frequentDfsCodes
      .union(
        partialResults
          .union(refinementResults)
          .groupBy(0)
          .sum(1)
          .filter(new Frequent())
          .withBroadcastSet(minSupport, BroadcastNames.MIN_FREQUENCY)
      );

    return frequentDfsCodes;
  }


}
