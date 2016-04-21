package org.gradoop.examples.fsm;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.datagen.fsmtransactions.FSMTransactionGenerator;
import org.gradoop.datagen.fsmtransactions.FSMTransactionGeneratorConfig;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.GSpan;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.FileWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;

public class MultidimensionalBenchmark implements ProgramDescription {

  private static ExecutionEnvironment environment = ExecutionEnvironment
    .getExecutionEnvironment();

  private static Collection<String> stats = new ArrayList<>();
  private static FileWriter writer;

  public static void main(String[] args) throws Exception {
    writer = new FileWriter("stats.csv");
    writer.close();

    int loops = Integer.valueOf(args[0]);
    String filePrefix = args[1];

    FSMTransactionGeneratorConfig config;

    int[] minVertexCounts = new int[] {10, 30, 100};
    int[] maxVertexCounts = new int[] {20, 60, 200};
    int[] minEdgeCounts = new int[] {30, 90, 300};
    int[] maxEdgeCounts = new int[] {60, 180, 600};

    for(int i = 0; i < minVertexCounts.length; i++) {
      config = getDefaultConfig();
      config.setMinVertexCount(minVertexCounts[i]);
      config.setMaxVertexCount(maxVertexCounts[i]);
      config.setMinEdgeCount(minEdgeCounts[i]);
      config.setMaxEdgeCount(maxEdgeCounts[i]);

      runFor(config, "graphSize", filePrefix, loops);
    }

    int[] vertexLabelCounts = new int[] {20, 5, 3};
    int[] edgeLabelCounts = new int[] {20, 5, 3};

    for(int i = 0; i < vertexLabelCounts.length; i++) {
      config = getDefaultConfig();
      config.setVertexLabelCount(vertexLabelCounts[i]);
      config.setEdgeLabelCount(edgeLabelCounts[i]);

      runFor(config, "labelCount", filePrefix, loops);
    }

    int[] vertexLabelSizes = new int[] {1, 10, 100};
    int[] edgeLabelSizes = new int[] {1, 10, 100};

    for(int i = 0; i < vertexLabelSizes.length; i++) {
      config = getDefaultConfig();
      config.setVertexLabelSize(vertexLabelSizes[i]);
      config.setEdgeLabelSize(edgeLabelSizes[i]);

      runFor(config, "labelSize", filePrefix, loops);
    }

    int[] graphCounts = new int[] {1000, 10000, 100000};

    for(int graphCount : graphCounts) {
      config = getDefaultConfig();
      config.setGraphCount(graphCount);

      runFor(config, "graphCount", filePrefix, loops);
    }
  }

  @SuppressWarnings("unchecked")
  private static void runFor(FSMTransactionGeneratorConfig genConf,
    String series, String filePrefix, int loops) throws
    Exception {

    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> gradoopConf =
      GradoopFlinkConfig.createDefaultConfig(environment);

    FSMTransactionGenerator<GraphHeadPojo, VertexPojo, EdgePojo>
      generator = new FSMTransactionGenerator<>(gradoopConf, genConf);

    String vertexFile = filePrefix + "vertices.json";
    String edgeFile = filePrefix + "edges.json";
    String graphFile = filePrefix + "graphs.json";

    float[] thresholds = new float[] {0.9f, 0.7f, 0.5f, 0.3f};

    for(int i = 1; i<= loops; i++) {
      generator
        .execute()
        .writeAsJson(
          vertexFile, edgeFile, graphFile, FileSystem.WriteMode.OVERWRITE);

      for(float threshold : thresholds) {
        GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> input = EPGMDatabase
          .fromJsonFile(vertexFile, edgeFile, graphFile, gradoopConf)
          .getCollection();

        GSpan<GraphHeadPojo, VertexPojo, EdgePojo> miner =
        new GSpan<>(FSMConfig.forDirectedMultigraph(threshold));

        long patCount = miner.execute(input).getGraphHeads().count();

        Files.append(series + ";" +
            genConf.toString() + ";" +
            threshold + ";" +
            patCount + ";" +
            environment.getLastJobExecutionResult().getNetRuntime() + "\n",
          FileUtils.getFile("stats.csv"),
          Charset.defaultCharset()
        );
      }
    }
  }

  private static FSMTransactionGeneratorConfig getDefaultConfig() {
    return new FSMTransactionGeneratorConfig(
      10000, // graph count
      30,  // min vertex count
      60,  // max vertex count
      5,   // vertex label count
      10,   // vertex label size
      90,  // min edge count
      180, // max edge count
      5,  // edgeLabelCount,
      10   // edgeLabelSize
    );
  }

  @Override
  public String getDescription() {
    return "FSMBench";
  }
}
