package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.comparison.DfsCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsEmbedding;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsStep;
import org.gradoop.model.impl.algorithms.fsm.tuples.SearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GrowEmbeddings
  extends RichMapFunction<SearchSpaceItem, SearchSpaceItem> {


  public static final String DS_NAME = "DFSs";
  private CompressedDfsCode[] frequentDfsCodes;
  private final DfsCodeComparator comparator;

  public GrowEmbeddings(FSMConfig fsmConfig) {
    this.comparator = new DfsCodeComparator(fsmConfig.isDirected());
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentDfsCodes = getRuntimeContext()
      .<CompressedDfsCode[]>getBroadcastVariable(DS_NAME).get(0);
  }

  @Override
  public SearchSpaceItem map(SearchSpaceItem graph) throws Exception {

    // min DFS code per subgraph (set of edge ids)
    Map<GradoopIdSet, DfsCode> coverageMinDfsCode = new HashMap<>();
    // embeddings per DFS code
    Map<DfsCode, Collection<DfsEmbedding>> codeEmbeddings = new HashMap<>();

    // for each supported frequent DFS code
    for(Map.Entry<CompressedDfsCode, Collection<DfsEmbedding>> entry :
      graph.getCodeEmbeddings().entrySet()) {

      CompressedDfsCode compressedDfsCode = entry.getKey();
      if(ArrayUtils.contains(frequentDfsCodes, compressedDfsCode)) {

        DfsCode parentDfsCode = compressedDfsCode.getDfsCode();

        // for each embedding
        for(DfsEmbedding parentEmbedding : entry.getValue()) {

          // for each vertex of embedding
          for(GradoopId fromVertexId : parentEmbedding.getVertexTimes()) {

            // for each incident edge
            for(AdjacencyListEntry adjacencyListEntry :
              graph.getAdjacencyLists().get(fromVertexId).getEntries()) {

              GradoopId edgeId = adjacencyListEntry.getEdgeId();
              GradoopId toVertexId = adjacencyListEntry.getVertexId();

              // if grow possible
              if(!parentEmbedding.getEdgeTimes().contains(edgeId)) {
                DfsEmbedding embedding = DfsEmbedding.deepCopy(parentEmbedding);
                DfsCode dfsCode = DfsCode.deepCopy(parentDfsCode);

                Integer fromVertexTime = parentEmbedding
                  .getVertexTimes().indexOf(fromVertexId);

                Integer toVertexTime = parentEmbedding
                  .getVertexTimes().indexOf(toVertexId);

                // if forward
                if(toVertexTime >= 0) {
                  embedding.getVertexTimes().add(toVertexId);
                  toVertexTime = embedding.getVertexTimes().size();
                }

                dfsCode.getSteps().add(new DfsStep(
                    fromVertexTime,
                    adjacencyListEntry.getVertexLabel(),
                    adjacencyListEntry.isOutgoing(),
                    adjacencyListEntry.getEdgeLabel(),
                    toVertexTime,
                    adjacencyListEntry.getVertexLabel()
                ));

                embedding.getEdgeTimes().add(edgeId);

                // check if subgraph already discovered
                GradoopIdSet coverage =
                  GradoopIdSet.fromExisting(embedding.getEdgeTimes());

                // update min DFS code if subgraph not already discovered or
                // new DFS code is less than last one minimum one
                DfsCode minDfsCode = coverageMinDfsCode.get(coverage);
                Collection<DfsEmbedding> embeddings = codeEmbeddings
                  .get(dfsCode);

                if(minDfsCode == null
                  || comparator.compare(dfsCode, minDfsCode) < 0) {

                  coverageMinDfsCode.put(coverage, dfsCode);

                  if(minDfsCode != null) {
                    codeEmbeddings.remove(minDfsCode);
                  }
                }

                if(embeddings == null) {
                  codeEmbeddings.put(dfsCode, Lists.newArrayList(embedding));
                } else {
                  embeddings.add(embedding);
                }
              }
            }
          }
        }
      }
    }

    HashMap<CompressedDfsCode, Collection<DfsEmbedding>>
      compressedCodeEmbeddings = new HashMap<>();

    for(Map.Entry<DfsCode, Collection<DfsEmbedding>> entry :
      codeEmbeddings.entrySet()) {

      compressedCodeEmbeddings.put(
        new CompressedDfsCode(entry.getKey()),
        entry.getValue()
      );
    }

    graph.setCodeEmbeddings(compressedCodeEmbeddings);

    return graph;
  }
}
