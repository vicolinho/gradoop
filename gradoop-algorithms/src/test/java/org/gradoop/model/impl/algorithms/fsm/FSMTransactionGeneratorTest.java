package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.fsmtransactions.FSMTransactionGeneratorConfig;
import org.gradoop.datagen.fsmtransactions.PredictableFSMTransactionGenerator;
import org.gradoop.datagen.fsmtransactions.RandomFSMTransactionGenerator;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common
  .GradoopTransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.FilterRefineGSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.iterative.IterativeGSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.functions.utils.WithEmptySide;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

public class FSMTransactionGeneratorTest extends GradoopFlinkTestBase {

  @Test
  public void testPredictableGenerator() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      new PredictableFSMTransactionGenerator<>(getConfig(), 100)
      .execute();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.95f);

    GradoopTransactionalFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GradoopTransactionalFSMEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(input, fsmConfig);

    TransactionalFSMiner iMiner = new IterativeGSpanMiner();
    iMiner.setExecutionEnvironment(
      input.getConfig().getExecutionEnvironment());
    DataSet<WithCount<CompressedSubgraph>> iResult =
      iMiner.mine(edges, encoder.getMinSupport(), fsmConfig);

    TransactionalFSMiner frMiner = new FilterRefineGSpanMiner();
    DataSet<WithCount<CompressedSubgraph>> frResult =
      frMiner.mine(edges, encoder.getMinSupport(), fsmConfig);

    iResult.fullOuterJoin(frResult)
      .where(0).equalTo(0)
      .with(new WithEmptySide<WithCount<CompressedSubgraph>, WithCount<CompressedSubgraph>>())
      .print();
  }

  @Test
  public void testRandomGenerator() throws Exception {
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

    RandomFSMTransactionGenerator<GraphHeadPojo, VertexPojo, EdgePojo> gen =
      new RandomFSMTransactionGenerator<>(getConfig(), generatorConfig);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      gen.execute();

//    input.getConfig().getExecutionEnvironment().setParallelism(1);

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.3f);

    GradoopTransactionalFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GradoopTransactionalFSMEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(input, fsmConfig);

    TransactionalFSMiner iMiner = new IterativeGSpanMiner();
    iMiner.setExecutionEnvironment(
      input.getConfig().getExecutionEnvironment());
    DataSet<WithCount<CompressedSubgraph>> iResult =
      iMiner.mine(edges, encoder.getMinSupport(), fsmConfig);

    TransactionalFSMiner frMiner = new FilterRefineGSpanMiner();
    DataSet<WithCount<CompressedSubgraph>> frResult =
      frMiner.mine(edges, encoder.getMinSupport(), fsmConfig);

    iResult.fullOuterJoin(frResult)
      .where(0).equalTo(0)
      .with(new WithEmptySide<WithCount<CompressedSubgraph>, WithCount<CompressedSubgraph>>())
      .print();
  }

}