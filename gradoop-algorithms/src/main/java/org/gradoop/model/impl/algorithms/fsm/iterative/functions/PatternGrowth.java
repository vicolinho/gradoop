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

package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

/**
 * Core of gSpan implementation. Grows embeddings of KnownToBeGloballyFrequent DFS codes.
 */
public class PatternGrowth
  implements MapFunction<IterationItem, IterationItem> {

  private final FSMConfig fsmConfig;

  /**
   * constructor
   * @param fsmConfig configuration
   */
  public PatternGrowth(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public IterationItem map(IterationItem wrapper) throws Exception {

    if (! wrapper.isCollector()) {
      GSpan.growEmbeddings(
        wrapper.getTransaction(), fsmConfig.isDirected());
    }

    return wrapper;
  }
}
