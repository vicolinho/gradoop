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

//    System.out.println("EXPECTED");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
//    System.out.println("RESULT");
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
      "s4[(:A)-[:a]->(:B)-[:b]->(:C)]" +
      "s5[(:A)-[:a]->(:B)-[:c]->(:E)]" ;

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("s1", "s2", "s3", "s4", "s5");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      gSpan.execute(searchSpace);

//    System.out.println("EXPECTED");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
//    System.out.println("RESULT");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(result);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }

  @Test
  public void testParallelEdges() throws Exception {
    GSpan<GraphHeadPojo, VertexPojo, EdgePojo>
      gSpan = new GSpan<>(FSMConfig.forDirectedMultigraph(0.7f));

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(:A)-[:a]->(v1:A)]" +
      "g2[(v2:A)-[:a]->(:A)-[:a]->(v2:A)]" +
      "g3[(:A)-[:a]->(:A)-[:a]->(:A)]" +
      "s1[(:A)-[:a]->(:A)]" +
      "s2[(v3:A)-[:a]->(:A)-[:a]->(v3:A)]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("s1", "s2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      gSpan.execute(searchSpace);

//    System.out.println("EXPECTED");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
//    System.out.println("RESULT");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(result);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }

  @Test
  public void testLoop() throws Exception {
    GSpan<GraphHeadPojo, VertexPojo, EdgePojo>
      gSpan = new GSpan<>(FSMConfig.forDirectedMultigraph(0.7f));

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v1)-[:a]->(:A)]" +
      "g2[(v2:A)-[:a]->(v2)-[:a]->(:A)]" +
      "g3[(:A)-[:a]->(:A)-[:a]->(:A)]" +
      "s1[(:A)-[:a]->(:A)]" +
      "s2[(v3:A)-[:a]->(v3)]" +
      "s3[(v4:A)-[:a]->(v4)-[:a]->(:A)]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("s1", "s2", "s3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      gSpan.execute(searchSpace);

//    System.out.println("EXPECTED");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
//    System.out.println("RESULT");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(result);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }

  @Test
  public void testDiamond() throws Exception {
    GSpan<GraphHeadPojo, VertexPojo, EdgePojo>
      gSpan = new GSpan<>(FSMConfig.forDirectedMultigraph(0.7f));

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
      "g2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
      "g3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

      "s1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

      "s2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)             ]" +
      "s3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);             (v3:A)-[:a]->(v4:A)]" +
      "s4[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A)                                 ]" +
      "s5[(v1:A)-[:a]->(v2:A)             ;(v1:A)-[:a]->(v3:A)             ]" +
      "s6[             (v2:A)-[:a]->(v4:A);             (v3:A)-[:a]->(v4:A)]" +
      "s7[(v1:A)-[:a]->(v2:A)                                              ]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables(
        "s1", "s2", "s3", "s4", "s5", "s6", "s7");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      gSpan.execute(searchSpace);

//    System.out.println("EXPECTED");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
//    System.out.println("RESULT");
//    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(result);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }

  @Test
  public void testCircleWithBranch() throws Exception {
    GSpan<GraphHeadPojo, VertexPojo, EdgePojo>
      gSpan = new GSpan<>(FSMConfig.forDirectedMultigraph(0.7f));

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
      "g2[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
      "g3[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +

      "s41[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)-[:b]->(:B)]" +
      "s31[(v1:A)-[:a]->(:A)-[:a]->(:A)-[:a]->(v1)           ]" +
      "s32[(v1:A)-[:a]->(:A)-[:a]->(:A)       (v1)-[:b]->(:B)]" +
      "s33[(v1:A)-[:a]->(:A)       (:A)-[:a]->(v1)-[:b]->(:B)]" +
      "s34[             (:A)-[:a]->(:A)-[:a]->(v1:A)-[:b]->(:B)]" +

      "s21[(:A)-[:a]->(:A)-[:a]->(:A)]" +
      "s22[(:A)-[:a]->(:A)-[:b]->(:B)]" +
      "s23[(:A)<-[:a]-(:A)-[:b]->(:B)]" +

      "s11[(:A)-[:a]->(:A)]" +
      "s12[(:A)-[:b]->(:B)]";


      FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables(
        "s11", "s12", "s21", "s22", "s23", "s31", "s32", "s33", "s34", "s41");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      gSpan.execute(searchSpace);

    System.out.println("EXPECTED");
    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(expectation);
    System.out.println("RESULT");
    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(result);

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}