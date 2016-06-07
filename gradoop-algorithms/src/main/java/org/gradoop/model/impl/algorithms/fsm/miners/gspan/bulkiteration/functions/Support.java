package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.tuples.WithCount;

/**
 * Created by peet on 02.06.16.
 */
public class Support<T>
  implements GroupCombineFunction<T, WithCount<T>> {


  @Override
  public void combine(Iterable<T> iterable, Collector<WithCount<T>> collector) throws Exception {

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

    collector.collect(new WithCount<>(object, support));

  }
}
