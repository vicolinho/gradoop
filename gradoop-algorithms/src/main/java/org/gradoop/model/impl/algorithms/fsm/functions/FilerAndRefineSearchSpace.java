package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * Created by peet on 22.04.16.
 */
public class FilerAndRefineSearchSpace
  implements JoinFunction<
  Tuple2<GradoopId,ArrayList<Tuple2<GradoopId, Integer>>>,
  Tuple2<GradoopId, ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>,
  AdjacencyLists> {

  @Override
  public AdjacencyLists join(
    Tuple2<GradoopId, ArrayList<Tuple2<GradoopId, Integer>>> graphVertices,
    Tuple2<GradoopId, ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>
      graphEdges) throws Exception {



    return null;
  }
}
