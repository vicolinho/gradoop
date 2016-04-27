package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;

import java.util.ArrayList;

public class AddTaskId extends RichMapFunction<ArrayList<FatEdge>,
  Tuple2<Integer, ArrayList<FatEdge>>> {

  private Integer subtaskId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.subtaskId = getRuntimeContext().getIndexOfThisSubtask();
  }

  @Override
  public Tuple2<Integer, ArrayList<FatEdge>> map(
    ArrayList<FatEdge> fatEdges) throws Exception {

    return new Tuple2<>(this.subtaskId, fatEdges);
  }
}
