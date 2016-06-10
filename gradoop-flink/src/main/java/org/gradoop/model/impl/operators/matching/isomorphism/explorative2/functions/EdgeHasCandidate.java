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

package org.gradoop.model.impl.operators.matching.isomorphism.explorative2.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.operators.matching.common.query.Step;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.ExplorativeSubgraphIsomorphism;


import org.gradoop.model.impl.operators.matching.simulation.dual.tuples
  .TripleWithDirection;


/**
 * Filters edge triples if their candidates contain a given candidate. This
 * method can only be applied during iteration as the candidate depends on the
 * current super step.
 *
 * Read fields:
 *
 * f3: direction
 * f4: candidates
 */
@FunctionAnnotation.ForwardedFields("*")
@FunctionAnnotation.ReadFields("f3;f4")
public class EdgeHasCandidate
  extends RichFilterFunction<TripleWithDirection> {
  /**
   * Traversal code
   */
  private final TraversalCode traversalCode;
  /**
   * Candidate to test on
   */
  private int candidate;
  /**
   * Direction to test on
   */
  private boolean isOutgoing;

  /**
   * Constructor
   *
   * @param traversalCode traversal code to determine candidate
   */
  public EdgeHasCandidate(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    int superstep = (int) getRuntimeContext()
      .getBroadcastVariable(ExplorativeSubgraphIsomorphism.BC_SUPERSTEP).get(0);
    Step step   = traversalCode.getStep(superstep - 1);
    candidate   = (int) step.getVia();
    isOutgoing  = step.isOutgoing();
  }

  @Override
  public boolean filter(TripleWithDirection t) throws Exception {
    return (t.isOutgoing() == isOutgoing) && t.getCandidates()[candidate];
  }
}
