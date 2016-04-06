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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

/**
 *  Minimal tuple-representation of an labeled vertex
 *  f0 : vertex id
 *  f1 : vertex label
 */
public class SimpleVertex extends Tuple2<GradoopId, String> {

  /**
   * default constructor
   */
  public SimpleVertex() {
  }

  /**
   * valued constructor
   * @param id vertex id
   * @param label vertex label
   */
  public SimpleVertex(GradoopId id, String label) {
    this.f0 = id;
    this.f1 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public String getLabel() {
    return this.f1;
  }
}
