package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.tuple.Tuple1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressedDfsCode extends Tuple1<byte[]> {

  public CompressedDfsCode() {}

  public CompressedDfsCode(DfsCode dfsCode) {

    try {
      ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
      GZIPOutputStream gzipOS = new GZIPOutputStream(byteArrayOS);
      ObjectOutputStream objectOS = new ObjectOutputStream(gzipOS);
      objectOS.writeObject(dfsCode);
      objectOS.close();
      this.f0 = byteArrayOS.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public DfsCode getDfsCode() {
    DfsCode dfsCode = null;

    try {
      ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(this.f0);
      GZIPInputStream gzipIn = new GZIPInputStream(byteArrayIS);
      ObjectInputStream objectIn = new ObjectInputStream(gzipIn);
      dfsCode = (DfsCode) objectIn.readObject();
      objectIn.close();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }

    return dfsCode;
  }

  @Override
  public String toString() {
    return getDfsCode().toString();
  }

  @Override
  public int hashCode() {

    HashCodeBuilder builder = new HashCodeBuilder();

    for (byte b : this.f0) {
      builder.append(b);
    }

    return builder.hashCode();

  }

  @Override
  public boolean equals(Object o) {
    return this.hashCode() == o.hashCode();
  }


}
