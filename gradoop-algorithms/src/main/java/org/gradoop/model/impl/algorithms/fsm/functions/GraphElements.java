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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.Collection;

/**
 * [(GraphId, GraphElement),..] => (GraphId, [GraphElement,..])
 * @param <EL>
 */
public class GraphElements<EL> implements GroupReduceFunction
  <Tuple2<GradoopId, EL>, Tuple2<GradoopId, Collection<EL>>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, EL>> iterable,
    Collector<Tuple2<GradoopId, Collection<EL>>> collector) throws Exception {

    Boolean first = true;
    GradoopId graphId = null;
    Collection<EL> elements = new ArrayList<>();

    for (Tuple2<GradoopId, EL> pair : iterable) {
      if (first) {
        first = false;
        graphId = pair.f0;
      }

      elements.add(pair.f1);
    }

    collector.collect(new Tuple2<>(graphId, elements));
  }
}
