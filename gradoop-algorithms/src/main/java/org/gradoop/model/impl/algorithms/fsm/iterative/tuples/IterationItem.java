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

package org.gradoop.model.impl.algorithms.fsm.iterative.tuples;

import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;

import java.io.Serializable;
import java.util.Collection;

public class IterationItem implements Serializable {


  private final GSpanTransaction transaction;
  private final Collection<CompressedDFSCode> frequentSubgraphs;
  private final boolean lastIteration;

  public IterationItem(GSpanTransaction transaction) {
    this.transaction = transaction;
    this.frequentSubgraphs = null;
    this.lastIteration = false;
  }

  public IterationItem(
    Collection<CompressedDFSCode> frequentSubgraphs, boolean lastIteration) {
    this.transaction = null;
    this.frequentSubgraphs = frequentSubgraphs;
    this.lastIteration = lastIteration;
  }

  public boolean isTransaction() {
    return transaction != null;
  }

  public Boolean isCollector() {
    return frequentSubgraphs != null;
  }

  public boolean isLastIteration() {
    return lastIteration;
  }

  public GSpanTransaction getTransaction() {
    return this.transaction;
  }

  public Collection<CompressedDFSCode> getFrequentSubgraphs() {
    return this.frequentSubgraphs;
  }
}
