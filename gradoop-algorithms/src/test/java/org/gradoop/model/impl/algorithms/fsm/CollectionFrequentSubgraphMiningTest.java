package org.gradoop.model.impl.algorithms.fsm;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class CollectionFrequentSubgraphMiningTest extends GradoopFlinkTestBase {

  @Test
  public void testSingleEdges() throws Exception {

    GSpan<GraphHeadPojo, VertexPojo, EdgePojo>
      gSpan = new GSpan<>(FSMConfig.forDirectedMultigraph(0.7f));

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

//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(result);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }

  @Test
  public void testSimpleGraphs() throws Exception {
    GSpan<GraphHeadPojo, VertexPojo, EdgePojo>
      gSpan = new GSpan<>(FSMConfig.forDirectedMultigraph(0.7f));

    String asciiGraphs = "" +
      "g1[(:A)-[:a]->(v1:B)-[:b]->(:C);(v1)-[:c]->(:D)]" +
      "g2[(:A)-[:a]->(v2:B)-[:b]->(:C);(v2)-[:c]->(:E)]" +
      "g3[(:A)-[:a]->(v3:B)-[:d]->(:C);(v3)-[:c]->(:E)]" +
      "s1[(:A)-[:a]->(:B)]" +
      "s2[(:B)-[:b]->(:C)]" +
      "s3[(:B)-[:c]->(:E)]" +
      "s4[(:A)-[:a]->(:B)-[:b]->(:C)]" ;

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("s1", "s2", "s3", "s4");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      gSpan.execute(searchSpace);

//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(result);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}