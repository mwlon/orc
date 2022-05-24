package org.apache.orc.impl;

import io.github.mwlon.qcompress.QCompress;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.io.IOException;
import java.util.ArrayList;

public class QCompressLongReader implements IntegerReader {
  private static final int BUF_SIZE = 4096;

  private final InStream inStream;
  private long[] values;
  private boolean computed = false;
  private int longPos = 0;

  public QCompressLongReader(InStream inStream) {
    this.inStream = inStream;
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    throw new IOException("not yet supported");
  }

  @Override
  public void skip(long numValues) throws IOException {
    throw new IOException("not yet supported");
  }

  private void computeIfNeeded() throws IOException {
    if (!computed) {
      ArrayList<Byte> bytes = new ArrayList<>();
      byte[] buf = new byte[BUF_SIZE];
      while (true) {
        int len = inStream.read(buf);
        if (len == -1) {
          break;
        }

        for (int i = 0; i < len; i++) {
          bytes.add(buf[i]);
        }
      }
      byte[] byteArr = new byte[bytes.size()];
      for (int i = 0; i < bytes.size(); i++) {
        byteArr[i] = bytes.get(i);
      }
      this.values = QCompress.autoDecompressLongs(byteArr);
      this.computed = true;
    }
  }

  @Override
  public long next() throws IOException {
    computeIfNeeded();
    long res = values[longPos];
    longPos++;
    return res;
  }

  @Override
  public boolean hasNext() throws IOException {
    computeIfNeeded();
    return longPos < values.length;
  }

  @Override
  public void nextVector(ColumnVector previous,
                         long[] data,
                         int previousLen) throws IOException {
    // if all nulls, just return
    if (previous.isRepeating && !previous.noNulls && previous.isNull[0]) {
      return;
    }
    previous.isRepeating = true;
    for (int i = 0; i < previousLen; i++) {
      if (previous.noNulls || !previous.isNull[i]) {
        data[i] = next();
      } else {
        // The default value of null for int type in vectorized
        // processing is 1, so set that if the value is null
        data[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && (data[0] != data[i] ||
          previous.isNull[0] != previous.isNull[i])) {
        previous.isRepeating = false;
      }
    }
  }

  @Override
  public void nextVector(ColumnVector vector, int[] data, int size) throws IOException {
    final int batchSize = Math.min(data.length, size);
    if (vector.noNulls) {
      for (int r = 0; r < batchSize; ++r) {
        data[r] = (int) next();
      }
    } else if (!(vector.isRepeating && vector.isNull[0])) {
      for (int r = 0; r < batchSize; ++r) {
        data[r] = (vector.isNull[r]) ? 1 : (int) next();
      }
    }
  }
}
