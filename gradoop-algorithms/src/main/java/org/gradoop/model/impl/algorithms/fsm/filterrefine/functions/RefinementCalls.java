package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.RefinementMessage;

import java.util.Collection;
import java.util.Iterator;

public class RefinementCalls implements
  GroupReduceFunction<RefinementMessage, Tuple2<Integer, Collection<CompressedSubgraph>>> {

  @Override
  public void reduce(
    Iterable<RefinementMessage> iterable,
    Collector<Tuple2<Integer, Collection<CompressedSubgraph>>> collector) throws
    Exception {

    Iterator<RefinementMessage> iterator = iterable.iterator();

    RefinementMessage message = iterator.next();

    int workerId = message.getWorkerId();

    Collection<CompressedSubgraph> codes = Lists
      .newArrayList(message.getSubgraph());

    while (iterator.hasNext()) {
      message = iterator.next();
      codes.add(message.getSubgraph());
    }
    collector.collect(new Tuple2<>(workerId, codes));

  }

}
