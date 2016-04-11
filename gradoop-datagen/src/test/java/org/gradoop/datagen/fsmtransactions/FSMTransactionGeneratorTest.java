package org.gradoop.datagen.fsmtransactions;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

public class FSMTransactionGeneratorTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {
    FSMTransactionGeneratorConfig generatorConfig =
      new FSMTransactionGeneratorConfig(
        10, // graph count
        3,  // min vertex count
        9,  // max vertex count
        2,  // min edge count
        20  // max edge count
      );

    FSMTransactionGenerator<GraphHeadPojo, VertexPojo, EdgePojo> gen =
      new FSMTransactionGenerator<>(getConfig(), generatorConfig);

    GradoopFlinkTestUtils.printCanonicalAdjacencyMatrix(
      gen.execute()
    );
  }

}