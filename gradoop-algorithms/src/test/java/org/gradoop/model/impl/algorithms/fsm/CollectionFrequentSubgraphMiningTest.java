package org.gradoop.model.impl.algorithms.fsm;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class CollectionFrequentSubgraphMiningTest extends GradoopFlinkTestBase {

  @Test
  public void testSingleEdges() throws Exception {

    GSpan<GraphHeadPojo, VertexPojo, EdgePojo> gSpan = new GSpan<>(0.7f);

    String asciiGraphs = "" +
      "g1[(v1:A)-[e1:a]->(v2:A)];" +
      "g2[(v1)-[e1]->(v2)];" +
      "g3[(:A)-[:a]->(:A);(:B)-[:b]->(:B);(:B)-[:b]->(:B)]" +
      "g4[(:A)-[:b]->(:A);(:A)-[:b]->(:A);(:A)-[:b]->(:A)];" +
      "s1[(:A)-[:a]->(:A)]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1", "g2", "g3", "g4");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("s1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      gSpan.execute(searchSpace);

    //collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}