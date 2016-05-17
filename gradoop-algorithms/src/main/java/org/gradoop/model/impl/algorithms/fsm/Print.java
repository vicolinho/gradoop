package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.common.functions.MapFunction;

public class Print<T> implements
  MapFunction<T, T> {

  private final String prefix;

  public Print(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public T map(T t) throws Exception {
    System.out.println(prefix + " : " + t);
    return t;
  }
}
