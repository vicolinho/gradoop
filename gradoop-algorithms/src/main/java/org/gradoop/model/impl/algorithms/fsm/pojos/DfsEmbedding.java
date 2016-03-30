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

  @Override
  public String toString() {
    return "\n\t\t\tVt : " + vertexTimes + "\n\t\t\t" + "Et : " + edgeTimes;
  }
}
