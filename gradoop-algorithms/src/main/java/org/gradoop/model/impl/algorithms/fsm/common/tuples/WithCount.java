package org.gradoop.model.impl.algorithms.fsm.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;

public class WithCount<T> extends Tuple2<T, Integer> {

  public WithCount() {

  }

  public WithCount(T t) {
    super(t, 1);
  }

  public WithCount(T t, int support) {
    super(t, support);
  }

  public Integer getSupport() {
    return f1;
  }

  public T getObject() {
    return f0;
  }

  public void setSupport(int support) {
    this.f1 = support;
  }
}
