package org.gradoop.model.impl.operators.tostring;

import org.gradoop.model.GradoopFlinkTestBase;

public class MinDFSCodeBuilderTest extends GradoopFlinkTestBase {

//  @Test
//  public void testExecute() throws Exception {
//
//    GraphHeadToString<GraphHeadPojo> graphHeadToString = new
//      GraphHeadToDataString<>();
//
//    VertexToString<VertexPojo> vertexToString = new VertexToDataString<>();
//
//    EdgeToString<EdgePojo> edgeToString = new EdgeToDataString<>();
//
//    MinDFSCodeBuilder<GraphHeadPojo, VertexPojo, EdgePojo> builder = new
//      MinDFSCodeBuilder<>(graphHeadToString, vertexToString, edgeToString);
//
//    String asciiGraphs = "" +
//      "g1:G{x=1}[(a:A)-[:a]->(:B)-[:b]->(c:C);(a)-[:a]->(:B)-[:b]->(c)];";
//
//    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
//      getLoaderFromString(asciiGraphs);
//
//    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c12 =
//      loader.getGraphCollectionByVariables("g1");
//
//    builder.execute(c12).print();
//
//  }
}