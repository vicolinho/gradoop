package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;

import java.util.Collection;

public class SearchSpacePartition extends RichMapPartitionFunction
  <GSpanTransaction, Tuple2<Integer, Collection<GSpanTransaction>>> {

  @Override
  public void mapPartition(Iterable<GSpanTransaction> iterable,
    Collector<Tuple2<Integer, Collection<GSpanTransaction>>> collector) throws
    Exception {

    int workerId = getRuntimeContext().getIndexOfThisSubtask();
    Collection<GSpanTransaction> transactions = Lists.newArrayList(iterable);

    collector.collect(new Tuple2<>(workerId, transactions));
  }
}
