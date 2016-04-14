package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

public class Project0And3Of4<T0, T1, T2, T3> implements
  MapFunction<Tuple4<T0, T1, T2, T3>, Tuple2<T0,  T3>> {

  @Override
  public Tuple2<T0, T3> map(Tuple4<T0, T1, T2, T3> quadruple) throws
    Exception {
    return new Tuple2<>(quadruple.f0, quadruple.f3);
  }
}
