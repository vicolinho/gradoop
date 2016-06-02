package org.gradoop.model.impl.algorithms.fsm.filterrefine;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.common.AbstractTransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.functions.*;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.SubgraphMessage;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;

import java.util.Collection;
import java.util.Map;


public class FilterRefineTransactionalFSMiner
  extends AbstractTransactionalFSMiner {


  @Override
  public DataSet<ObjectWithCount<CompressedSubgraph>> mine(DataSet<EdgeTriple> edges,
                                                      DataSet<Integer> minSupport, FSMConfig fsmConfig) {

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
    DataSet<SubgraphMessage> fsmResult =
      partitions
        // run local FSM
        .flatMap(new LocalTransactionalFSM(fsmConfig));

    DataSet<SubgraphMessage> filterResult =
      fsmResult
        // group reports by DFS code
        .groupBy(0)
        // keep if sure or likely globally frequent; drop otherwise
        .reduceGroup(new FrequentOrRefinementCandidate(fsmConfig))
        .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
        .withBroadcastSet(workerIdsGraphCount, BroadcastNames.WORKER_GRAPHCOUNT);

    // add globally frequent DFS codes to result
    DataSet<ObjectWithCount<CompressedSubgraph>> frequentDfsCodes = filterResult
      .filter(new KnownToBeGloballyFrequent())
      .map(new ToSupportable());

    // REFINEMENT

    DataSet<SubgraphMessage> refinementCandidates =
      filterResult
        .filter(new NeedsRefinement());

    // remember incomplete results
    DataSet<ObjectWithCount<CompressedSubgraph>> incompleteResults = refinementCandidates
      .filter(new IncompleteResult())
      .map(new ToSupportable());

    // get refined results
//    DataSet<Supportable<CompressedDfsCode>> refinementResults = refinementCandidates
//      .filter(new RefinementCall())
//      .groupBy(1)
//      .reduceGroup(new RefinementCalls())
//      .join(partitions)
//      .where(0).equalTo(0)
//      .with(new Refinement(fsmConfig));

    frequentDfsCodes = frequentDfsCodes
      .union(incompleteResults
//        .union(refinementResults)
        .groupBy(0)
        .sum(1)
        .filter(new Frequent())
        .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      );

    return frequentDfsCodes;
  }


}
