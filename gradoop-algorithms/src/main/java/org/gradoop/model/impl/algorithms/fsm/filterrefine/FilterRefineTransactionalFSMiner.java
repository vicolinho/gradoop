package org.gradoop.model.impl.algorithms.fsm.filterrefine;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.Print;
import org.gradoop.model.impl.algorithms.fsm.common.AbstractTransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.functions.*;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;
import org.gradoop.model.impl.functions.bool.False;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Map;


public class FilterRefineTransactionalFSMiner
  extends AbstractTransactionalFSMiner {

  @Override
  public DataSet<CompressedDFSCode> mine(
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges,
    DataSet<Integer> minSupport, FSMConfig fsmConfig) {

    // determine 1-edge frequent DFS codes
    DataSet<CompressedDFSCode> allFrequentDfsCodes =
      find1EdgeFrequentDfsCodes(fatEdges, minSupport);

    // filter edges by 1-edge DFS code
    fatEdges = filterFatEdges(fatEdges, allFrequentDfsCodes);

    if(fsmConfig.getMinEdgeCount() > 1) {
      allFrequentDfsCodes = allFrequentDfsCodes
        .filter(new False<CompressedDFSCode>());
    }

    if(fsmConfig.getMaxEdgeCount() != 1) {
      // distribute graphs to workers
      DataSet<Tuple2<Integer, Map<Integer, Transaction>>> partitions = fatEdges
        // group by graphId and create transaction for each graph
        .groupBy(0)
        .reduceGroup(new SearchSpace())
        // partition transactions
        .rebalance()
        .mapPartition(new SearchSpacePartition());

      // get worker ids with local graph counts
      DataSet<Map<Integer, Integer>> workerIdsGraphCount = partitions
        .map(new WorkerIdGraphCount())
        .reduceGroup(new WorkerIdsGraphCounts());

      workerIdsGraphCount = workerIdsGraphCount.map(new Print<Map<Integer,
        Integer>>("counts"));

      // FILTER round
      DataSet<Tuple3<CompressedDFSCode, Integer, Boolean>> fsmResult = partitions
        // run local FSM
        .flatMap(new LocalTransactionalFSM(fsmConfig));

//      fsmResult = fsmResult.map(new Print<Tuple3<CompressedDFSCode, Integer,
//        Boolean>>("report"));

      DataSet<Tuple3<CompressedDFSCode, Integer, Boolean>> filterResult =
        fsmResult
          // group reports by DFS code
          .groupBy("0.0")
          // keep if sure or likely globally frequent; drop otherwise
          .reduceGroup(new FrequentOrRefinementCandidate(fsmConfig))
          .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
          .withBroadcastSet(
            workerIdsGraphCount, BroadcastNames.WORKER_GRAPHCOUNT);

//      filterResult = filterResult.map(
//        new Print<Tuple3<CompressedDFSCode, Integer, Boolean>>("cand"));

      // add globally frequent DFS codes to result
      allFrequentDfsCodes = filterResult
        .filter(new KnownToBeGloballyFrequent())
        .map(new Value0Of3<CompressedDFSCode, Integer, Boolean>())
        .union(allFrequentDfsCodes);

      // REFINEMENT

      DataSet<Tuple3<CompressedDFSCode, Integer, Boolean>> refinementCandidates =
        filterResult
          .filter(new NeedsRefinement());

      // remember incomplete results
      DataSet<CompressedDFSCode> incompleteResults = refinementCandidates
        .filter(new IncompleteResult())
        .map(new Value0Of3<CompressedDFSCode, Integer, Boolean>());

      // get refined results
      DataSet<CompressedDFSCode> refinementResults = refinementCandidates
        .filter(new RefinementCall())
        .groupBy(1)
        .reduceGroup(new RefinementCalls())
        .join(partitions)
        .where(0).equalTo(0)
        .with(new Refinement(fsmConfig));

      allFrequentDfsCodes = allFrequentDfsCodes
        .union(
          incompleteResults
            .union(refinementResults)
            .groupBy(0)
            .sum(1)
            .filter(new Frequent())
            .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
        );
    }


    return allFrequentDfsCodes;
  }
}
