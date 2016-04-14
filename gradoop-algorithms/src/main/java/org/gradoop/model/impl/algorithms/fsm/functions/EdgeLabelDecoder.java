package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeLabelDecoder
  implements JoinFunction<Tuple4<GradoopId, GradoopId, GradoopId, Integer>,
  Tuple2<Integer, String>, Tuple4<GradoopId, GradoopId, GradoopId, String>> {


  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, String> join(
    Tuple4<GradoopId, GradoopId, GradoopId, Integer> gidSidTidLabel,
    Tuple2<Integer, String> dictionaryEntry
  ) throws Exception {

    return new Tuple4<>(gidSidTidLabel.f0,
      gidSidTidLabel.f1, gidSidTidLabel.f2, dictionaryEntry.f1);
  }
}
