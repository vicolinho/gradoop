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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * (GraphId, [Edge,..], [Edge,..]) => [Edge,..]
 * @param <G> graph type
 */
public class ExpandEdges<G extends EPGMGraphHead>
  implements FlatMapFunction
  <Tuple3<G, ArrayList<IntegerLabeledVertex>, ArrayList<IntegerLabeledEdge>>,
    Tuple2<GradoopId, IntegerLabeledEdge>> {

  @Override
  public void flatMap(
    Tuple3<G, ArrayList<IntegerLabeledVertex>, ArrayList<IntegerLabeledEdge>>
      triple,
    Collector<Tuple2<GradoopId, IntegerLabeledEdge>> collector) throws Exception {

    GradoopId graphId = triple.f0.getId();

    for(IntegerLabeledEdge edge : triple.f2) {
      collector.collect(new Tuple2<>(graphId, edge));
    }
  }
}
