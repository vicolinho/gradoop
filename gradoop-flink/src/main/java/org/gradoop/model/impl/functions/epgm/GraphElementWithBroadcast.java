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

package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Takes an object of type EPGMGraphElement, and creates a tuple2 of a
 * grapdoop id and the element. A tuple2 is generated for an object,
 * if the id is contained in the set of the object and the id satisfies
 * the containment predicate based on the broadcast set.
 * element => (graphId, element)
 * @param <El> graph element type
 */
public class GraphElementWithBroadcast<El extends EPGMGraphElement>
  extends RichFlatMapFunction<El, Tuple2<GradoopId, EPGMGraphElement>> {

  /**
   * name of broadcast variable
   */
  public static final String GRAPH_IDS = "graphIds";

  /**
   * reuse tuple2
   */
  private Tuple2<GradoopId, EPGMGraphElement> reuse;
  /**
   * true element is contained in the graph with an id that is NOT
   * contained in the broadcast dataset
   */
  private boolean isExclusion;

  /**
   *
   * @param isExclusion true -> element is contained in the graph with an id
   * that is NOT contained in the broadcast dataset
   */
  public GraphElementWithBroadcast(final boolean isExclusion) {
    this.isExclusion = isExclusion;
    reuse = new Tuple2<>();
  }
  @Override
  public void flatMap(El element, Collector<Tuple2<GradoopId,
    EPGMGraphElement>> out) throws Exception {
    List<GradoopId> graphIds = this.getRuntimeContext().
            getBroadcastVariable(GRAPH_IDS);
    for (GradoopId id :element.getGraphIds()) {
      if (!graphIds.contains(id) == this.isExclusion) {
        reuse.f0 = id;
        reuse.f1 = element;
        out.collect(reuse);
      }
    }
    out.close();
  }
}
