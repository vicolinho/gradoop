package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledVertex;
import org.gradoop.model.impl.id.GradoopId;

public class VertexLabelEncoder
  implements JoinFunction<Tuple3<GradoopId, GradoopId, String>,
  Tuple2<String, Integer>, Tuple3<GradoopId, GradoopId, Integer>> {


  @Override
  public Tuple3<GradoopId, GradoopId, Integer> join(
    Tuple3<GradoopId, GradoopId, String> gidVidLabel,
    Tuple2<String, Integer> dictionaryEntry
  ) throws Exception {

    return new Tuple3<>(gidVidLabel.f0, gidVidLabel.f1, dictionaryEntry.f1);
  }
}
