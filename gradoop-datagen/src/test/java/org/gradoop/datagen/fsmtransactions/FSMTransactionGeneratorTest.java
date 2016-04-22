package org.gradoop.datagen.fsmtransactions;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.IterativeGSpan;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class FSMTransactionGeneratorTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {
    FSMTransactionGeneratorConfig generatorConfig =
      new FSMTransactionGeneratorConfig(
        100, // graph count
        10,  // min vertex count
        20,  // max vertex count
        5, 1, 20,  // min edge count
        50, // max edge count
        // vertex label count
        // vertex label size
        5,  // edgeLabelCount,
        1   // edgeLabelSize
      );

    FSMTransactionGenerator<GraphHeadPojo, VertexPojo, EdgePojo> gen =
      new FSMTransactionGenerator<>(getConfig(), generatorConfig);

    IterativeGSpan gSpan = new IterativeGSpan<>(
      FSMConfig.forDirectedMultigraph(0.3f));

    System.out.println("Data started");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      gen.execute();

    System.out.println("Data finished, starting FSM");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> frequentSubgraphs =
      gSpan.execute(collection);

    System.out.println("FSM finished");

    assertTrue(frequentSubgraphs.getGraphHeads().count() > 0);
  }

}