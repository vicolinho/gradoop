package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;

public class Project6To0And3And4And5<T, T1, T2, T3, T4, T5> implements
  MapFunction<Tuple6<T, T1, T2, T3, T4, T5>, Tuple4<T, T3, T4, T5>> {
  @Override
  public Tuple4<T, T3, T4, T5> map(

    Tuple6<T, T1, T2, T3, T4, T5> heptuple) throws Exception {
    return new Tuple4<>(heptuple.f0, heptuple.f3, heptuple.f4, heptuple.f5);
  }
}
