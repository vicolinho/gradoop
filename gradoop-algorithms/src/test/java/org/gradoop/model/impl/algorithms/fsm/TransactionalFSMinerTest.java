package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.datagen.fsmtransactions.FSMTransactionGeneratorConfig;
import org.gradoop.datagen.fsmtransactions.PredictableFSMTransactionGenerator;
import org.gradoop.datagen.fsmtransactions.RandomFSMTransactionGenerator;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common
  .GradoopTransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.common.PrintDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.filterrefine
  .FilterRefineTransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.iterative
  .IterativeTransactionalFSMiner;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

public class TransactionalFSMinerTest   extends GradoopFlinkTestBase {

  @Test
  public void testIterative() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      new PredictableFSMTransactionGenerator<>(getConfig(), 100)
        .execute();

//    getExecutionEnvironment().setParallelism(3);

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.7f);
    int edgeCount = 2;
    fsmConfig.setMinEdgeCount(edgeCount);
    fsmConfig.setMaxEdgeCount(edgeCount);

    GradoopTransactionalFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GradoopTransactionalFSMEncoder<>();

    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges =
      encoder.encode(input, fsmConfig);

    TransactionalFSMiner iMiner = new IterativeTransactionalFSMiner();
    iMiner.setExecutionEnvironment(
      input.getConfig().getExecutionEnvironment());
    DataSet<CompressedDFSCode> iResult =
      iMiner.mine(fatEdges, encoder.getMinSupport(), fsmConfig);

    iResult
      .filter(new WrongCount())
      .map(new PrintDfsCode())
      .withBroadcastSet(
        encoder.getVertexLabelDictionary(),
        BroadcastNames.VERTEX_DICTIONARY)
      .withBroadcastSet(
        encoder.getEdgeLabelDictionary(),
        BroadcastNames.EDGE_DICTIONARY)
      .print();
  }

  @Test
  public void testIterativeVsFilterRefine() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input =
      new PredictableFSMTransactionGenerator<>(getConfig(), 100)
        .execute();

//    getExecutionEnvironment().setParallelism(3);

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.55f);
//    int edgeCount = 2;
//    fsmConfig.setMinEdgeCount(edgeCount);
//    fsmConfig.setMaxEdgeCount(edgeCount);

    GradoopTransactionalFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GradoopTransactionalFSMEncoder<>();

    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges =
      encoder.encode(input, fsmConfig);

    TransactionalFSMiner iMiner = new IterativeTransactionalFSMiner();
    iMiner.setExecutionEnvironment(
      input.getConfig().getExecutionEnvironment());
    DataSet<CompressedDFSCode> iResult =
      iMiner.mine(fatEdges, encoder.getMinSupport(), fsmConfig);

    TransactionalFSMiner frMiner = new FilterRefineTransactionalFSMiner();
    DataSet<CompressedDFSCode> frResult =
      frMiner.mine(fatEdges, encoder.getMinSupport(), fsmConfig);

    collectAndAssertTrue(
      And.reduce(
        iResult.fullOuterJoin(frResult)
          .where(0).equalTo(0)
          .with(new EqualSupport())
          .withBroadcastSet(
            encoder.getVertexLabelDictionary(),
            BroadcastNames.VERTEX_DICTIONARY)
          .withBroadcastSet(
            encoder.getEdgeLabelDictionary(),
            BroadcastNames.EDGE_DICTIONARY)
      )
    );
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

    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges =
      encoder.encode(input, fsmConfig);

    TransactionalFSMiner iMiner = new IterativeTransactionalFSMiner();
    iMiner.setExecutionEnvironment(
      input.getConfig().getExecutionEnvironment());
    DataSet<CompressedDFSCode> iResult =
      iMiner.mine(fatEdges, encoder.getMinSupport(), fsmConfig);

    TransactionalFSMiner frMiner = new FilterRefineTransactionalFSMiner();
    DataSet<CompressedDFSCode> frResult =
      frMiner.mine(fatEdges, encoder.getMinSupport(), fsmConfig);

    collectAndAssertTrue(
      And.reduce(
        iResult.fullOuterJoin(frResult)
          .where(0).equalTo(0)
          .with(new EqualSupport())
          .withBroadcastSet(
            encoder.getVertexLabelDictionary(),
            BroadcastNames.VERTEX_DICTIONARY)
          .withBroadcastSet(
            encoder.getEdgeLabelDictionary(),
            BroadcastNames.EDGE_DICTIONARY)
      )
    );
  }

  private class WrongCount implements FilterFunction<CompressedDFSCode> {
    @Override
    public boolean filter(CompressedDFSCode compressedDFSCode) throws
      Exception {
      return compressedDFSCode.getSupport() % 10 != 0;
    }
  }
}