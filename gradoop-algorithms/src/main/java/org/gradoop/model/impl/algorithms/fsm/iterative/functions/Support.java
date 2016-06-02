package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;

/**
 * Created by peet on 02.06.16.
 */
public class Support<T>
  implements GroupCombineFunction<T, ObjectWithCount<T>> {


  @Override
  public void combine(Iterable<T> iterable, Collector<ObjectWithCount<T>> collector) throws Exception {

    int support = 0;
    boolean first = true;
    T object = null;

    for(T t : iterable) {
      if (first) {
        object = t;
        first = false;
      }

      support++;
    }

    collector.collect(new ObjectWithCount<>(object, support));

  }
}
