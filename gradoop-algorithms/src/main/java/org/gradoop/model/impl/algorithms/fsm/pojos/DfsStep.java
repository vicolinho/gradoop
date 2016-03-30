package org.gradoop.model.impl.algorithms.fsm.pojos;


import java.io.Serializable;
import java.util.Objects;

public class DfsStep implements Serializable {

  private final Integer fromTime;
  private final String fromLabel;
  private final Boolean forward;
  private final String edgeLabel;
  private final Integer toTime;
  private final String toLabel;

  public DfsStep(Integer fromTime, String fromLabel, Boolean forward,
    String edgeLabel, Integer toTime, String toLabel) {
    this.fromTime = fromTime;
    this.fromLabel = fromLabel;
    this.forward = forward;
    this.edgeLabel = edgeLabel;
    this.toTime = toTime;
    this.toLabel = toLabel;
  }

  @Override
  public String toString() {
    return "(" + fromTime + ":" + fromLabel + ")" +
      (forward ? "" : "<") + "-" + edgeLabel + "-" + (forward ? ">" : "") +
      "(" + toTime + ":" + toLabel + ")";
  }

  public Integer getFromTime() {
    return fromTime;
  }

  public String getFromLabel() {
    return fromLabel;
  }

  public Boolean isForward() {
    return forward;
  }

  public String getEdgeLabel() {
    return edgeLabel;
  }

  public Integer getToTime() {
    return toTime;
  }

  public String getToLabel() {
    return toLabel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DfsStep step = (DfsStep) o;

    if (!getFromTime().equals(step.getFromTime())) {
      return false;
    }
    if (!getFromLabel().equals(step.getFromLabel())) {
      return false;
    }
    if (!isForward().equals(step.isForward())) {
      return false;
    }
    if (!getEdgeLabel().equals(step.getEdgeLabel())) {
      return false;
    }
    if (!getToTime().equals(step.getToTime())) {
      return false;
    }
    return getToLabel().equals(step.getToLabel());

  }

  @Override
  public int hashCode() {
    int result = getFromTime().hashCode();
    result = 31 * result + getFromLabel().hashCode();
    result = 31 * result + isForward().hashCode();
    result = 31 * result + getEdgeLabel().hashCode();
    result = 31 * result + getToTime().hashCode();
    result = 31 * result + getToLabel().hashCode();
    return result;
  }

  public Boolean isLoop() {
    return Objects.equals(fromTime, toTime);
  }
}
