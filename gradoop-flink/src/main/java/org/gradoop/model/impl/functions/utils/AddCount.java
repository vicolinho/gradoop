package org.gradoop.model.impl.functions.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.tuples.WithCount;

public class AddCount<T> implements MapFunction<T, WithCount<T>> {
  @Override
  public WithCount<T> map(T t) throws Exception {
    return new WithCount<>(t);
  }
}
