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

package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Minimal tuple-representation of an labeled edge
 * f0 : edge id
 * f1 : source vertex id
 * f2 : target vertex id
 * f4 : edge label
 */
public class LabeledEdge<L extends Comparable<L>>
  extends Tuple4<GradoopId, GradoopId, GradoopId, L> {

  /**
   * default constructor
   */
  public LabeledEdge() {
  }

  /**
   *
   * @param id edge id
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param label edge label
   */
  public LabeledEdge(
    GradoopId id, GradoopId sourceId, GradoopId targetId, L label) {
    this.f0 = id;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public GradoopId getTargetId() {
    return this.f2;
  }

  public L getLabel() {
    return this.f3;
  }
}
