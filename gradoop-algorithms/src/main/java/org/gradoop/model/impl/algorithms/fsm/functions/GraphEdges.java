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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * (graphId, sourceId, targetId, edgeLabel),..
 * => (graphId, [(sourceId, targetId, edgeLabel),..])
 */
public class GraphEdges implements
  GroupReduceFunction<Tuple4<GradoopId, GradoopId, GradoopId, Integer>,
    Tuple2<GradoopId, ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>> {

  @Override
  public void reduce(
    Iterable<Tuple4<GradoopId, GradoopId, GradoopId, Integer>> iterable,
    Collector<Tuple2<GradoopId,
      ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>> collector) throws
    Exception {

    boolean first = true;
    GradoopId graphId = null;
    ArrayList<Tuple3<GradoopId, GradoopId, Integer>> vertices =
      Lists.newArrayList();

    for (Tuple4<GradoopId, GradoopId, GradoopId, Integer> vertex : iterable) {
      if (first) {
        graphId = vertex.f0;
        first = false;
      }
      vertices.add(new Tuple3<>(vertex.f1, vertex.f2, vertex.f3));
    }
    collector.collect(new Tuple2<>(graphId, vertices));
  }
}
