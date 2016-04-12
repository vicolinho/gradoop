package org.gradoop.datagen.fsmtransactions;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.GSpanDeltaIteration;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

public class FSMTransactionGeneratorTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {
    FSMTransactionGeneratorConfig generatorConfig =
      new FSMTransactionGeneratorConfig(
        2000, // graph count
        10,  // min vertex count
        20,  // max vertex count
        20,  // min edge count
        50, // max edge count
        5,  // vertex label count
        1,  // vertex label size
        5,  // edgeLabelCount,
        1   // edgeLabelSize
      );

    FSMTransactionGenerator<GraphHeadPojo, VertexPojo, EdgePojo> gen =
      new FSMTransactionGenerator<>(getConfig(), generatorConfig);

    GSpanBulkIteration gSpan = new GSpanBulkIteration<>(
      FSMConfig.forDirectedMultigraph(0.3f));

    System.out.println("Data started");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      gen.execute();

    System.out.println("Data finished, starting FSM");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> frequentSubgraphs =
      gSpan.execute(collection);

    System.out.println("FSM finished");

    System.out.println(frequentSubgraphs.getGraphHeads().count());


    //GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(frequentSubgraphs);
  }

}