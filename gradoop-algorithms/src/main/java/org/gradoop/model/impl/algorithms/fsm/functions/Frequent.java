package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;

public class Frequent extends RichFilterFunction<Tuple2<CompressedDfsCode,
  Integer>> {

  public static final String DS_NAME = "minCount";
  private Integer minCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minCount = getRuntimeContext()
      .<Integer>getBroadcastVariable(DS_NAME)
      .get(0);
  }

  @Override
  public boolean filter(Tuple2<CompressedDfsCode, Integer> c) throws Exception {
    return c.f1 >= minCount;
  }
}