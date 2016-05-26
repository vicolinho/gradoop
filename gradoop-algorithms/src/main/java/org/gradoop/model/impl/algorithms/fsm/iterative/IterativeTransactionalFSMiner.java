package org.gradoop.model.impl.algorithms.fsm.iterative;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.gradoop.model.impl.algorithms.fsm.common.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples
  .IntegerLabeledEdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions.AddAll;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions
  .CollectedSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions
  .CreateCollector;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions
  .CreateEmptyCollector;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions
  .GrowFrequentSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions
  .IsFrequentSubgraphCollector;
import org.gradoop.model.impl.algorithms.fsm.iterative.functions.IsTransaction;
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
      edgeTriples, DataSet<Integer> minSupport, FSMConfig fsmConfig) {

    // determine 1-edge frequent DFS patterns
    DataSet<CompressedDFSCode> allFrequentSubgraphs =
      singleEdgeFrequentSubgraphs(edgeTriples, minSupport);

    // filter edges by 1-edge DFS pattern
    edgeTriples = frequent(edgeTriples, allFrequentSubgraphs);

    if(fsmConfig.getMinEdgeCount() > 1) {
      allFrequentSubgraphs = allFrequentSubgraphs
        .filter(new False<CompressedDFSCode>());
    }

    // create search space with collector
    DataSet<IterationItem> searchSpace = edgeTriples
      .groupBy(0)
      .reduceGroup(new SearchSpace(fsmConfig))
      .map(new WrapInIterationItem())
      .union(env.fromElements(true).map(new CreateEmptyCollector()));

    // ITERATION HEAD
    int maxEdgeCount = fsmConfig.getMaxEdgeCount();
    int maxIterations = (maxEdgeCount > 0 ? maxEdgeCount : MAX_EDGE_COUNT) - 1;

    if(maxIterations > 0) {
      IterativeDataSet<IterationItem> workSet = searchSpace
        .iterate(maxIterations);

      // ITERATION BODY

      // get last iteration frequent subgraphs
      DataSet<Collection<CompressedDFSCode>> lastFrequentSubgraphs = workSet
        .filter(new IsFrequentSubgraphCollector(true))
        .map(new CollectedSubgraphs());

      // get all frequent subgraphs
      DataSet<Collection<CompressedDFSCode>> multiEdgeFrequentSubgraphs =
        workSet
          .filter(new IsFrequentSubgraphCollector(false))
          .map(new CollectedSubgraphs());

      // grow frequent subgraphs
      DataSet<IterationItem> nextWorkSet = workSet
        .filter(new IsTransaction())
        .map(new GrowFrequentSubgraphs(fsmConfig))
        .withBroadcastSet(
          lastFrequentSubgraphs, BroadcastNames.LAST_FREQUENT_SUBGRAPHS)
        .filter(new HasGrownSubgraphs());

      // determine grown frequent subgraphs
      DataSet<Collection<CompressedDFSCode>> nextFrequentSubgraphs =
        nextWorkSet
          .flatMap(new ReportGrownSubgraphs())  // report patterns
          .groupBy(0)                           // group by pattern
          .sum(1)                               // count support
          .filter(new Frequent())               // filter by min support
          .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT)
          .flatMap(new PostPrune(fsmConfig))  // filter false positives
          .reduceGroup(new CollectFrequentSubgraphs());


      multiEdgeFrequentSubgraphs = multiEdgeFrequentSubgraphs
        .union(nextFrequentSubgraphs)
        .reduce(new AddAll());

      nextWorkSet = nextWorkSet
        .union(
          multiEdgeFrequentSubgraphs
          .map(new CreateCollector(false))
        )
        .union(
          nextFrequentSubgraphs
          .map(new CreateCollector(true))
        );

      // ITERATION FOOTER
      DataSet<IterationItem> collector = workSet
        // terminate, if no new frequent DFS patterns
        .closeWith(nextWorkSet, nextFrequentSubgraphs);

      // post processing
      allFrequentSubgraphs = collector
        .filter(new IsFrequentSubgraphCollector(false))
        .flatMap(new ExpandFrequentDfsCodes())
        .union(allFrequentSubgraphs);
    }

    return allFrequentSubgraphs;
  }
}
