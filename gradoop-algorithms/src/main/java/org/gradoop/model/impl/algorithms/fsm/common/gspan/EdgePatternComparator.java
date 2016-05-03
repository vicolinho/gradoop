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

package org.gradoop.model.impl.algorithms.fsm.common.gspan;


import org.gradoop.model.impl.algorithms.fsm.common.pojos.EdgePattern;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for edge patterns considering vertex labels, edge label and
 * direction (if directed == true).
 *
* @param <L> label type
 */
public class EdgePatternComparator<L extends Comparable<L>>
  implements Comparator<EdgePattern<L>>, Serializable {

  /**
   * true for comparing directed edge patterns, false for undirected
   */
  private final Boolean directed;

  /**
   * constructor
   * @param directed true for comparing directed edge patterns
   */
  public EdgePatternComparator(Boolean directed) {
    this.directed = directed;
  }

  @Override
  public int compare(EdgePattern<L> ep1, EdgePattern<L> ep2) {

    int comparison = ep1.getMinVertexLabel().compareTo(ep2.getMinVertexLabel());

    if (directed) {
      if (comparison == 0) {
        if (ep1.isOutgoing() && ep2.isOutgoing()) {
          comparison = 0;
        } else if (ep1.isOutgoing()) {
          comparison = -1;
        } else {
          comparison = 1;
        }
        if (comparison == 0) {
          comparison = ep1.getEdgeLabel().compareTo(ep2.getEdgeLabel());

          if (comparison == 0) {
            comparison = ep1.getMaxVertexLabel()
              .compareTo(ep2.getMaxVertexLabel());
          }
        }
      }
    } else {
      if (comparison == 0) {
        comparison = ep1.getEdgeLabel().compareTo(ep2.getEdgeLabel());

        if (comparison == 0) {
          comparison = ep1.getMaxVertexLabel()
            .compareTo(ep2.getMaxVertexLabel());
        }
      }
    }
    return comparison;
  }
}
