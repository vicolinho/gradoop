package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpacePartition;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;

import java.util.ArrayList;

/**
 * Created by peet on 22.04.16.
 */
public class SearchSpacePartitioner
  implements GroupCombineFunction
  <ArrayList<Tuple2<FatEdge, CompressedDFSCode>>, SearchSpacePartition> {

  @Override
  public void combine(
    Iterable<ArrayList<Tuple2<FatEdge, CompressedDFSCode>>> iterable,
    Collector<SearchSpacePartition> collector) throws Exception {

    ArrayList<ArrayList<Tuple2<FatEdge, CompressedDFSCode>>> partition = Lists
      .newArrayList();

    for(ArrayList<Tuple2<FatEdge, CompressedDFSCode>> graph : iterable) {
      partition.add(graph);
    }

    collector.collect(new SearchSpacePartition(partition));
  }
}
