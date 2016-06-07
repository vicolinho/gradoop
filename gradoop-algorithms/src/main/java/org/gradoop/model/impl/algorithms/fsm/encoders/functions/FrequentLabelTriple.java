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

package org.gradoop.model.impl.algorithms.fsm.encoders.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.LabelTripleWithCount;

/**
 * (label, support) => support >= minFrequency
 */
public class FrequentLabelTriple
  extends RichFilterFunction<LabelTripleWithCount> {

  /**
   * name of broadcast data set for minimum support
   */
  private static final String DS_NAME = "minFrequency";
  /**
   * minimum support
   */
  private Integer minSupport;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minSupport = getRuntimeContext()
      .<Integer>getBroadcastVariable(DS_NAME)
      .get(0);
  }


  @Override
  public boolean filter(LabelTripleWithCount tripleWithSupport
  ) throws Exception {
    return tripleWithSupport.getSupport() >= minSupport;
  }
}
