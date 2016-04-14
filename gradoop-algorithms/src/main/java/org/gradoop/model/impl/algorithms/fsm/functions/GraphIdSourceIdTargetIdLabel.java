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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * EPGMVertex => (graphId, sourceId, targetId, edgeLabel),..
 * @param <E> edge type
 */
public class GraphIdSourceIdTargetIdLabel<E extends EPGMEdge> implements
  FlatMapFunction<E,
  Tuple4<GradoopId, GradoopId, GradoopId, String>> {
  @Override
  public void flatMap(E edge,
    Collector<Tuple4<GradoopId, GradoopId, GradoopId, String>> collector) throws
    Exception {

    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String label = edge.getLabel();

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(
        new Tuple4<>(graphId, sourceId, targetId, label));
    }
  }
}
