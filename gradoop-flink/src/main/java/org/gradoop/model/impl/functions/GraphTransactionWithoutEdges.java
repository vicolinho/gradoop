package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphTransaction;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 11.04.16.
 */
public class GraphTransactionWithoutEdges
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements FlatJoinFunction<G, Tuple2<GradoopId, V>, GraphTransaction<G, V, E>> {

  @Override
  public void join(G g, Tuple2<GradoopId, V> gradoopIdVTuple2,
    Collector<GraphTransaction<G, V, E>> collector) throws Exception {

  }
}
