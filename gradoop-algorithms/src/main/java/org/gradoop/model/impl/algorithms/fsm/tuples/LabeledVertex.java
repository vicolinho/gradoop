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
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

/**
 *  Minimal tuple-representation of an labeled vertex
 *  f0 : vertex id
 *  f1 : vertex label
 */
public class LabeledVertex<L extends Comparable<L>> extends Tuple2<GradoopId, L>
  implements GenericLabeled<L> {

  /**
   * default constructor
   */
  public LabeledVertex() {
  }

  /**
   * valued constructor
   * @param id vertex id
   * @param label vertex label
   */
  public LabeledVertex(GradoopId id, L label) {
    this.f0 = id;
    this.f1 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public void setId(GradoopId id) {
    this.f0 = id;
  }

  @Override
  public L getLabel() {
    return this.f1;
  }

  @Override
  public void setLabel(L label) {
    this.f1 = label;
  }

}
