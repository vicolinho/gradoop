/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.GSpanBase;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.GrowFrequentSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.IsCollector;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.IsTransaction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.IterationItemWithCollector;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.IterationItemWithTransaction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.SubgraphCollection;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.ExpandSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.HasGrownSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.PostPruneAndCompress;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions.ReportGrownSubgraphs;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.SerializedSubgraph;

import org.gradoop.model.impl.functions.utils.AddCount;
import org.gradoop.model.impl.functions.utils.SumCount;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.pojos.IterationItem;

import java.util.Collection;

/**
 * gSpan implementation based on Flink Bulk Iteration.
 */
public class GSpanBulkIteration extends GSpanBase {

  @Override
  public DataSet<WithCount<CompressedSubgraph>> mine(DataSet<EdgeTriple> edges,
    DataSet<Integer> minFrequency, FsmConfig fsmConfig) {

    setFsmConfig(fsmConfig);
    DataSet<IterationItem> transactions = createTransactions(edges)
      .map(new IterationItemWithTransaction());
//      .map(new Print<IterationItem>(""));

    // create search space with collector

    Collection<WithCount<CompressedSubgraph>> emptySubgraphList =
      Lists.newArrayListWithExpectedSize(0);

    DataSet<IterationItem> searchSpace = transactions
      .union(env
        .fromElements(emptySubgraphList)
        .map(new IterationItemWithCollector())
        .returns(TypeInformation.of(IterationItem.class)));

    // ITERATION HEAD
    IterativeDataSet<IterationItem> workSet = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    // determine grown frequent subgraphs
    transactions = workSet
      .filter(new IsTransaction());

    // report ,filter and validate frequent subgraphs
    DataSet<WithCount<CompressedSubgraph>> currentFrequentSubgraphs =
      transactions
        .flatMap(new ReportGrownSubgraphs())
        .map(new AddCount<SerializedSubgraph>())
        // count frequency per worker, prune and compress subgraph
        .groupBy(0)
        .combineGroup(new SumCount<SerializedSubgraph>())
        .flatMap(new PostPruneAndCompress(fsmConfig))
        // count global frequency and filter frequent subgraphs
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<CompressedSubgraph>())
        .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY);

    // get all frequent subgraphs
    DataSet<Collection<WithCount<CompressedSubgraph>>> collector = workSet
      .filter(new IsCollector())
      .map(new SubgraphCollection())
      .union(
        currentFrequentSubgraphs
          .reduceGroup(new SubgraphCollection(fsmConfig))
      )
      .reduce(new SubgraphCollection());

    // grow frequent subgraphs
    DataSet<IterationItem> nextWorkSet = transactions
      .map(new GrowFrequentSubgraphs(fsmConfig))
      .withBroadcastSet(
        currentFrequentSubgraphs, BroadcastNames.FREQUENT_SUBGRAPHS)
      .filter(new HasGrownSubgraphs())
      .union(
        collector
        .map(new IterationItemWithCollector())
      );

    // ITERATION FOOTER
    DataSet<IterationItem> resultSet = workSet
      // terminate, if no new frequent DFS patterns
      .closeWith(nextWorkSet, currentFrequentSubgraphs);

    return resultSet
      .filter(new IsCollector())
      .flatMap(new ExpandSubgraphs());
  }

}
