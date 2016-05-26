package org.gradoop.model.impl.algorithms.fsm.iterative;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.AbstractTransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.functions
  .ConcatFrequentPatterns;
import org.gradoop.model.impl.algorithms.fsm.common.functions.ExpandFrequentDfsCodes;
import org.gradoop.model.impl.algorithms.fsm.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.common.functions.IsActive;
import org.gradoop.model.impl.algorithms.fsm.common.functions.IsCollector;
import org.gradoop.model.impl.algorithms.fsm.common.functions.PostPruning;
import org.gradoop.model.impl.algorithms.fsm.common.functions.ReportPatterns;
import org.gradoop.model.impl.algorithms.fsm.common.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.common.functions.SupportPruning;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.IntegerLabeledEdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions
  .CreateCollector;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions.PatternGrowth;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions
  .WrapInIterationItem;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;


import org.gradoop.model.impl.functions.bool.False;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;

public class IterativeTransactionalFSMiner
  extends AbstractTransactionalFSMiner {

  @Override
  public DataSet<CompressedDFSCode> mine(
    DataSet<Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>>
      fatEdges, DataSet<Integer> minSupport, FSMConfig fsmConfig) {

    // determine 1-edge frequent DFS patterns
    DataSet<CompressedDFSCode> allFrequentDfsCodes =
      find1EdgeFrequentDfsCodes(fatEdges, minSupport);

    // filter edges by 1-edge DFS pattern
    fatEdges = filterFatEdges(fatEdges, allFrequentDfsCodes);

    if(fsmConfig.getMinEdgeCount() > 1) {
      allFrequentDfsCodes = allFrequentDfsCodes
        .filter(new False<CompressedDFSCode>());
    }

    // create search space with collector
    DataSet<IterationItem> searchSpace = fatEdges
      .groupBy(0)
      .reduceGroup(new SearchSpace(fsmConfig))
      .map(new WrapInIterationItem())
      .union(
        env.fromElements(true)
        .map(new CreateCollector())
      );

    // ITERATION HEAD
    int maxEdgeCount = fsmConfig.getMaxEdgeCount();
    int maxIterations = (maxEdgeCount > 0 ? maxEdgeCount : MAX_EDGE_COUNT) - 1;

    if(maxIterations > 0) {
      IterativeDataSet<IterationItem> workSet = searchSpace
        .iterate(maxIterations);

      // ITERATION BODY
      DataSet<IterationItem> activeWorkSet = workSet
        .map(new PatternGrowth(fsmConfig))  // grow supported embeddings
        .filter(new IsActive());            // active, if at least one growth

      // determine frequent DFS patterns
      DataSet<Collection<CompressedDFSCode>> currentFrequentPatterns =
        activeWorkSet
          .flatMap(new ReportPatterns())  // report patterns
          .groupBy(0)                     // group by pattern
          .sum(1)                         // count support
          .filter(new Frequent())         // filter by min support
          .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
          .flatMap(new PostPruning(fsmConfig))
          .reduceGroup(new ConcatFrequentPatterns());

      // grow children of frequent DFS patterns
      DataSet<IterationItem> nextWorkSet = activeWorkSet
        .map(new SupportPruning())  // drop embeddings of
        // infrequent patterns
        .withBroadcastSet(
          currentFrequentPatterns, BroadcastNames.FREQUENT_PATTERNS)
        .filter(new IsActive());    // active, if at least one frequent pattern

      // ITERATION FOOTER
      DataSet<IterationItem> collector = workSet
        // terminate, if no new frequent DFS patterns
        .closeWith(nextWorkSet, currentFrequentPatterns);

      // post processing
      allFrequentDfsCodes = collector
        .filter(new IsCollector())             // get only collector
        .flatMap(new ExpandFrequentDfsCodes()) // expand array to data set
        .union(allFrequentDfsCodes);
    }

    return allFrequentDfsCodes;
  }
}
