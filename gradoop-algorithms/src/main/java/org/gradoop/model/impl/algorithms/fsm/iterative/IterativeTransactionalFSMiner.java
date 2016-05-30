package org.gradoop.model.impl.algorithms.fsm.iterative;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.model.impl.algorithms.fsm.common
  .AbstractTransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.functions
  .CollectFrequentSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.common.functions
  .ExpandFrequentDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.common.functions.HasGrownSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.common.functions.PostPrune;
import org.gradoop.model.impl.algorithms.fsm.common.functions
  .ReportGrownSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions.*;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;

public class IterativeTransactionalFSMiner
  extends AbstractTransactionalFSMiner {

  @Override
  public DataSet<CompressedDFSCode> mine(DataSet<EdgeTriple> edges,
    DataSet<Integer> minSupport, FSMConfig fsmConfig) {

    setFsmConfig(fsmConfig);
    DataSet<IterationItem> transactions = createTransactions(edges)
      .map(new WrapTransactionInIterationItem());

    // create search space with collector

    Collection<CompressedDFSCode> emptySubgraphList =
      Lists.newArrayListWithExpectedSize(0);

    DataSet<IterationItem> searchSpace = transactions
      .union(env
        .fromElements(emptySubgraphList)
        .map(new WrapCollectorInIterationItem())
      );

    // ITERATION HEAD
    IterativeDataSet<IterationItem> workSet = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount() - 1);

    // ITERATION BODY

    // determine grown frequent subgraphs
    transactions = workSet
      .filter(new IsTransaction());

    // report and filter frequent subgraphs
    DataSet<CompressedDFSCode> currentFrequentSubgraphs = transactions
      .flatMap(new ReportGrownSubgraphs())  // report patterns
      .groupBy(0)                           // group by pattern
      .sum(1)                               // count support
      .filter(new Frequent())               // filter by min support
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
      .flatMap(new PostPrune(fsmConfig));   // filter false positives

    // get all frequent subgraphs
    DataSet<Collection<CompressedDFSCode>> collector = workSet
      .filter(new IsCollector())
      .map(new AllFrequentSubgraphs())
      .union(
        currentFrequentSubgraphs
          .reduceGroup(new CollectFrequentSubgraphs())
      )
      .reduce(new Merge());

    // determine leftmost branches (Pre-Pruning)
    currentFrequentSubgraphs = currentFrequentSubgraphs
      .map(new SetSupportToBranchNumber()) // (ab-) reuse of integer field
      .groupBy(1) // group by (min vertex label) branch number
      .reduceGroup(new MinimumDfsCode(fsmConfig));

    // grow frequent subgraphs
    DataSet<IterationItem> nextWorkSet = workSet
      .map(new GrowFrequentSubgraphs(fsmConfig))
      .withBroadcastSet(
        currentFrequentSubgraphs, BroadcastNames.FREQUENT_SUBGRAPHS)
      .filter(new HasGrownSubgraphs())
      .union(
        collector
        .map(new WrapCollectorInIterationItem())
      );

    // ITERATION FOOTER
    DataSet<IterationItem> resultSet = workSet
      // terminate, if no new frequent DFS patterns
      .closeWith(nextWorkSet, currentFrequentSubgraphs);

    return resultSet
      .filter(new IsCollector())
      .flatMap(new ExpandFrequentDfsCodes());
  }

}
