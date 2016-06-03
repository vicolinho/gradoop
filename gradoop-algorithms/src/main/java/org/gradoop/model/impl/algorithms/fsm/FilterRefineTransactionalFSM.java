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

package org.gradoop.model.impl.algorithms.fsm;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.TransactionalFSM;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.FilterRefineGSpanMiner;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class FilterRefineTransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TransactionalFSM<G, V, E> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public FilterRefineTransactionalFSM(FSMConfig fsmConfig) {
    super(fsmConfig, new FilterRefineGSpanMiner());
  }

  @Override
  public String getName() {
    return "Naive parallel Transactional FSM";
  }
}
