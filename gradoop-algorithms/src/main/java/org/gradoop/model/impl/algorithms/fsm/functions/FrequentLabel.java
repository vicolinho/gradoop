package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.id.GradoopId;

public class FrequentLabel
  extends RichFilterFunction<Tuple2<String, Integer>> {

    /**
     * name of broadcast data set for minimum support
     */
    public static final String DS_NAME = "minSupport";
    /**
     * minimum support
     */
    private Integer minSupport;

    @Override
    public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minSupport = getRuntimeContext()
      .<Integer>getBroadcastVariable(DS_NAME)
      .get(0);
  }



  @Override
  public boolean filter(
    Tuple2<String, Integer> labelSupport) throws Exception {
    return labelSupport.f1 >= minSupport;
  }
}
