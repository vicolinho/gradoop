package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeLabelEncoder
  implements JoinFunction<Tuple4<GradoopId, GradoopId, GradoopId, String>,
  Tuple2<String, Integer>, Tuple4<GradoopId, GradoopId, GradoopId, Integer>> {


  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, Integer> join(
    Tuple4<GradoopId, GradoopId, GradoopId, String> gidSidTidLabel,
    Tuple2<String, Integer> dictionaryEntry
  ) throws Exception {

    return new Tuple4<>(gidSidTidLabel.f0,
      gidSidTidLabel.f1, gidSidTidLabel.f2, dictionaryEntry.f1);
  }
}
