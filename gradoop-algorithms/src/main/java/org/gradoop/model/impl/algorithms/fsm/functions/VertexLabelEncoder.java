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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (graphId, vertexId, stringLabel) |><| (stringLabel, integerLabel)
 * => (graphId, vertexId, integerLabel)
 */
public class VertexLabelEncoder
  implements JoinFunction<Tuple3<GradoopId, GradoopId, String>,
  Tuple2<String, Integer>, Tuple3<GradoopId, GradoopId, Integer>> {
  @Override
  public Tuple3<GradoopId, GradoopId, Integer> join(
    Tuple3<GradoopId, GradoopId, String> gidVidLabel,
    Tuple2<String, Integer> dictionaryEntry
  ) throws Exception {

    return new Tuple3<>(gidVidLabel.f0, gidVidLabel.f1, dictionaryEntry.f1);
  }
}
