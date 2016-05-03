package org.gradoop.datagen.fsmtransactions;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.IterativeTransactionalFSM;
import org.gradoop.model.impl.algorithms.fsm.NaiveParallelTransactionalFSM;
import org.gradoop.model.impl.operators.equality.CollectionEquality;
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

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      gen.execute();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.3f);

    NaiveParallelTransactionalFSM<GraphHeadPojo, VertexPojo, EdgePojo> miner1 =
      new NaiveParallelTransactionalFSM<>(fsmConfig);

    IterativeTransactionalFSM<GraphHeadPojo, VertexPojo, EdgePojo> miner2 =
      new IterativeTransactionalFSM<>(fsmConfig);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result1 =
      miner1.execute(input);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result2 =
      miner2.execute(input);

//    assertTrue(result1.getGraphHeads().count() > 0);
//    assertTrue(result2.getGraphHeads().count() > 0);

    collectAndAssertTrue(result1.equalsByGraphElementData(result2));

  }

}