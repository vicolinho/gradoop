package org.gradoop.model.impl.algorithms.fsm.pojos;

import com.google.common.collect.Lists;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

public class DfsEmbedding {
  private final ArrayList<GradoopId> vertexTimes;
  private final ArrayList<GradoopId> edgeTimes;

  public DfsEmbedding(ArrayList<GradoopId> vertexTimes, GradoopId edgeId) {
    this.vertexTimes = vertexTimes;
    this.edgeTimes = Lists.newArrayList(edgeId);
  }

  public DfsEmbedding(ArrayList<GradoopId> vertexTimes,
    ArrayList<GradoopId> edgeTimes) {
    this.vertexTimes = vertexTimes;
    this.edgeTimes = edgeTimes;
  }

  @Override
  public String toString() {
    return "\n\t\t\tVt : " + vertexTimes + "\n\t\t\t" + "Et : " + edgeTimes;
  }

  public ArrayList<GradoopId> getVertexTimes() {
    return vertexTimes;
  }

  public ArrayList<GradoopId> getEdgeTimes() {
    return edgeTimes;
  }

  public static DfsEmbedding deepCopy(DfsEmbedding embedding) {
    return new DfsEmbedding(
      Lists.newArrayList(embedding.getVertexTimes()),
      Lists.newArrayList(embedding.getEdgeTimes())
    );
  }
}
