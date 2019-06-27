
package com.example;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;

public class FakeInputSplit extends InputSplit implements Writable {

  @Override
  public void readFields(DataInput arg0) {
  }

  @Override
  public void write(DataOutput arg0) {
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }
}
