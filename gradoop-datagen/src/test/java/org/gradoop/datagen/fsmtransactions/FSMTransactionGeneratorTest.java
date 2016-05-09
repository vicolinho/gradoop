package org.gradoop.datagen.fsmtransactions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.FilterRefineTransactionalFSM;
import org.gradoop.model.impl.algorithms.fsm.IterativeTransactionalFSM;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

public class FSMTransactionGeneratorTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {
    FSMTransactionGeneratorConfig generatorConfig =
      new FSMTransactionGeneratorConfig(
        100, // graph count
        10,  // min vertex count
        20,  // max vertex count
        5,   // vertex label count
        1,   // vertex label size
        20,  // min edge count
        50, // max edge count
        5,  // edgeLabelCount,
        1   // edgeLabelSize
      );

    FSMTransactionGenerator<GraphHeadPojo, VertexPojo, EdgePojo> gen =
      new FSMTransactionGenerator<>(getConfig(), generatorConfig);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      gen.execute();

//    input.getConfig().getExecutionEnvironment().setParallelism(1);

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.3f);

    FilterRefineTransactionalFSM<GraphHeadPojo, VertexPojo, EdgePojo>
      parallelMiner = new FilterRefineTransactionalFSM<>(fsmConfig);

    IterativeTransactionalFSM<GraphHeadPojo, VertexPojo, EdgePojo>
      iterativeMiner = new IterativeTransactionalFSM<>(fsmConfig);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> parallelResult =
      parallelMiner.execute(input);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> iterativeResult =
      iterativeMiner.execute(input);

    Count.count(parallelResult.getGraphHeads()).cross(
      Count.count(iterativeResult.getGraphHeads())
    ).with(new CrossFunction<Long, Long, String>() {
      @Override
      public String cross(Long aLong, Long aLong2) throws Exception {
        return aLong.toString() + "/" + aLong2.toString();
      }
    }).print();

  }

}