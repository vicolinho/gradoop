package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Project0And2Of3<T0, T1, T2> implements
  MapFunction<Tuple3<T0, T1, T2>, Tuple2<T0, T2>> {

  @Override
  public Tuple2<T0, T2> map(Tuple3<T0, T1, T2> tuple3) throws
    Exception {
    return new Tuple2<>(tuple3.f0, tuple3.f2);
  }
}
