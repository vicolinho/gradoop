package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphElements;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphSimpleEdge;
import org.gradoop.model.impl.algorithms.fsm.functions.GraphSimpleVertex;
import org.gradoop.model.impl.algorithms.fsm.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpaceItem;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;

public class GSpan
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  private final float threshold;
  protected DataSet<Long> minCount;
  private GradoopFlinkConfig<G, V, E> config;

  public GSpan(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection)  {

    this.config = collection.getConfig();
    DataSet<SearchSpaceItem> searchSpace = getSearchSpace(collection);


    try {
      searchSpace.print();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  private DataSet<SearchSpaceItem> getSearchSpace(
    GraphCollection<G, V, E> collection) {

    DataSet<Tuple2<GradoopId, Collection<SimpleVertex>>> graphVertices =
      collection
        .getVertices()
        .flatMap(new GraphSimpleVertex<V>())
        .groupBy(0)
        .reduceGroup(new GraphElements<SimpleVertex>());

    DataSet<Tuple2<GradoopId, Collection<SimpleEdge>>> graphEdges =
      collection
        .getEdges()
        .flatMap(new GraphSimpleEdge<E>())
        .groupBy(0)
        .reduceGroup(new GraphElements<SimpleEdge>());

    return graphVertices
      .join(graphEdges)
      .where(0).equalTo(0)
      .with(new SearchSpace());
  }


  @Override
  public String getName() {
    return null;
  }
}
