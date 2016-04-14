/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * creates a label dictionary based on support (max support = label 1)
 *
 * (label, support),.. => (label, 1), (label, 2)
 */
public class Dictionary implements
  GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

  @Override
  public void reduce(Iterable<Tuple2<String, Integer>> iterable,
    Collector<Tuple2<String, Integer>> collector) throws Exception {

    List<Tuple2<String, Integer>> list = Lists.newArrayList();

    for (Tuple2<String, Integer> labelSupport : iterable) {
      list.add(labelSupport);
    }

    Collections.sort(list, new Comparator<Tuple2<String, Integer>>() {
      @Override
      public int compare(Tuple2<String, Integer> o1,
        Tuple2<String, Integer> o2) {

        int comparison;

        if (o1.f1 > o2.f1) {
          comparison = -1;
        } else {
          comparison = o1.f0.compareTo(o2.f0);
        }

        return comparison;
      }
    });

    for (int i = 0; i < list.size(); i++) {
      collector.collect(new Tuple2<>(list.get(i).f0, i));
    }
  }
}
