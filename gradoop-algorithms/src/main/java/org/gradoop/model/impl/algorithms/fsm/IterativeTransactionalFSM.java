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
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.TransactionalFSM;
import org.gradoop.model.impl.algorithms.fsm.iterative.IterativeGSpanMiner;

/**
 * The gSpan frequent subgraph mining algorithm implemented as Gradoop Operator
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class IterativeTransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TransactionalFSM<G, V, E> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   */
  public IterativeTransactionalFSM(FSMConfig fsmConfig) {
    super(fsmConfig, new IterativeGSpanMiner());
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection) {
    this.miner.setExecutionEnvironment(
      collection.getConfig().getExecutionEnvironment());
    return super.execute(collection);
  }

  @Override
  public String getName() {
    return "Transactional FSM using Bulk Iteration";
  }
}
