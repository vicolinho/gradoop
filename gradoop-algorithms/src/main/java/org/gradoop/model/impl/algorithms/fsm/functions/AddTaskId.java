package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;

/**
 * Created by peet on 22.04.16.
 */
public class AddTaskId extends RichMapFunction<AdjacencyLists, Tuple2<Integer, AdjacencyLists>> {

  private Integer subtaskId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.subtaskId = getRuntimeContext().getIndexOfThisSubtask();
  }

  @Override
  public Tuple2<Integer, AdjacencyLists> map(AdjacencyLists adjacencyLists) throws
    Exception {
    return new Tuple2<>(this.subtaskId, adjacencyLists);
  }
}
