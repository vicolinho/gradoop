package org.gradoop.model.impl.algorithms.fsm.pojos;


import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Objects;

public class DfsStep implements Serializable {

  private final int fromTime;
  private final String fromLabel;
  private final Boolean outgoing;
  private final String edgeLabel;
  private final int toTime;
  private final String toLabel;

  public DfsStep(int fromTime, String fromLabel, Boolean outgoing,
    String edgeLabel, int toTime, String toLabel) {
    this.fromTime = fromTime;
    this.fromLabel = fromLabel;
    this.outgoing = outgoing;
    this.edgeLabel = edgeLabel;
    this.toTime = toTime;
    this.toLabel = toLabel;
  }

  @Override
  public String toString() {
    return "(" + fromTime + ":" + fromLabel + ")" +
      (outgoing ? "" : "<") + "-" + edgeLabel + "-" + (outgoing ? ">" : "") +
      "(" + toTime + ":" + toLabel + ")";
  }

  public int getFromTime() {
    return fromTime;
  }

  public String getFromLabel() {
    return fromLabel;
  }

  public Boolean isOutgoing() {
    return outgoing;
  }

  public String getEdgeLabel() {
    return edgeLabel;
  }

  public int getToTime() {
    return toTime;
  }

  public String getToLabel() {
    return toLabel;
  }

  @Override
  public boolean equals(Object obj) {
    boolean equals = obj == this;

    if(!equals && obj != null && obj.getClass() == getClass()) {

      DfsStep other = (DfsStep) obj;

      EqualsBuilder builder = new EqualsBuilder();

      builder.append(this.isOutgoing(), other.isOutgoing());
      builder.append(this.getFromTime(), other.getFromTime());
      builder.append(this.getToTime(), other.getToTime());
      builder.append(this.getFromLabel(), other.getFromLabel());
      builder.append(this.getEdgeLabel(), other.getEdgeLabel());
      builder.append(this.getToLabel(), other.getToLabel());

      equals = builder.isEquals();
    }

    return equals;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    builder.append(isOutgoing());
    builder.append(getFromTime());
    builder.append(getToTime());
    builder.append(getFromLabel());
    builder.append(getEdgeLabel());
    builder.append(getToLabel());

    return builder.hashCode();
  }

  public Boolean isLoop() {
    return fromTime == toTime;
  }

  public Boolean isForward() {
    return getFromTime() < getToTime() || getToTime() == 0;
  }

  public Boolean isBackward() {
    return !isForward();
  }
}
