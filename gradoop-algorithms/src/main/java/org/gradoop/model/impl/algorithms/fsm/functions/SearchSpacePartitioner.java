package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpacePartition;

import java.util.ArrayList;

/**
 * Created by peet on 22.04.16.
 */
public class SearchSpacePartitioner implements
  GroupCombineFunction<Tuple2<Integer, AdjacencyLists>, SearchSpacePartition> {

  @Override
  public void combine(Iterable<Tuple2<Integer, AdjacencyLists>> iterable,
    Collector<SearchSpacePartition> collector) throws Exception {

    ArrayList<AdjacencyLists> partition = Lists.newArrayList();

    for(Tuple2<Integer, AdjacencyLists> lists : iterable) {
      partition.add(lists.f1);
    }

    collector.collect(new SearchSpacePartition(partition));

  }
}
