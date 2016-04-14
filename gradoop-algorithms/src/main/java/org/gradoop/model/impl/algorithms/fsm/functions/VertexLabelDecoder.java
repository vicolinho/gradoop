package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

public class VertexLabelDecoder
  implements JoinFunction<Tuple3<GradoopId, GradoopId, Integer>,
  Tuple2<Integer, String>, Tuple3<GradoopId, GradoopId, String>> {


  @Override
  public Tuple3<GradoopId, GradoopId, String> join(
    Tuple3<GradoopId, GradoopId, Integer> gidVidLabel,
    Tuple2<Integer, String> dictionaryEntry
  ) throws Exception {

    return new Tuple3<>(gidVidLabel.f0, gidVidLabel.f1, dictionaryEntry.f1);
  }
}
