package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * Created by peet on 27.04.16.
 */
public class GraphFatEdges implements
  GroupReduceFunction<Tuple6<GradoopId, GradoopId, GradoopId, Integer,
    Integer, Integer>, ArrayList<FatEdge>> {
  @Override
  public void reduce(
    Iterable<Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>>
      iterable, Collector <ArrayList<FatEdge>>  collector
  ) throws Exception {

    ArrayList<FatEdge> out =
      Lists.newArrayList();

    for(Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>
        edge : iterable) {
      out.add(new FatEdge(edge.f1, edge.f2, edge.f3, edge.f4, edge.f5));
    }

    collector.collect(out);
  }
}
