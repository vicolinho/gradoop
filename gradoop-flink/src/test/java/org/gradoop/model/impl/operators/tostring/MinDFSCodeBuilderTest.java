package org.gradoop.model.impl.operators.tostring;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions
  .GraphHeadToDataString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class MinDFSCodeBuilderTest extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    GraphHeadToString<GraphHeadPojo> graphHeadToString = new
      GraphHeadToDataString<>();

    VertexToString<VertexPojo> vertexToString = new VertexToDataString<>();

    EdgeToString<EdgePojo> edgeToString = new EdgeToDataString<>();

    MinDFSCodeBuilder<GraphHeadPojo, VertexPojo, EdgePojo> builder = new
      MinDFSCodeBuilder<>(graphHeadToString, vertexToString, edgeToString);

    String asciiGraphs = "" +
      "g1:G{x=1}[(a:A)-[:a]->(:B)-[:b]->(c:C);(a)-[:a]->(:B)-[:b]->(c)];";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c12 =
      loader.getGraphCollectionByVariables("g1");

    builder.execute(c12).print();

  }
}