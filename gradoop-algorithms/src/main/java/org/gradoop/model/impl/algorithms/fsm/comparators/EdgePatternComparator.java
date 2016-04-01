package org.gradoop.model.impl.algorithms.fsm.comparators;

import org.gradoop.model.impl.algorithms.fsm.pojos.EdgePattern;

import java.io.Serializable;
import java.util.Comparator;

public class EdgePatternComparator
  implements Comparator<EdgePattern>, Serializable {
  private final Boolean directed;

  public EdgePatternComparator(Boolean directed) {
    this.directed = directed;
  }

  @Override
  public int compare(EdgePattern ep1, EdgePattern ep2) {

    int comparison = ep1.getMinVertexLabel()
      .compareTo(ep2.getMinVertexLabel());

    if(directed) {
      if(comparison == 0) {
        comparison = ep1.getOutgoing()
          .compareTo(ep2.getOutgoing());

        if(comparison == 0) {
          comparison = ep1.getEdgeLabel()
            .compareTo(ep2.getEdgeLabel());

          if(comparison == 0) {
            comparison = ep1.getMaxVertexLabel()
              .compareTo(ep2.getMaxVertexLabel());
          }
        }
      }
    } else {
      if(comparison == 0) {

        comparison = ep1.getEdgeLabel()
          .compareTo(ep2.getEdgeLabel());

        if(comparison == 0) {
          comparison = ep1.getMaxVertexLabel()
            .compareTo(ep2.getMaxVertexLabel());
        }
      }
    }

    return comparison;
  }
}
