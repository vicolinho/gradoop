package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;

public class MinCount implements MapFunction<Long, Integer> {

  private final float threshold;

  public MinCount(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public Integer map(Long totalCount) throws Exception {
    return Math.round(((float) totalCount * threshold));
  }
}
