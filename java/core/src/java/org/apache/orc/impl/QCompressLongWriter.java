package org.apache.orc.impl;

import io.github.mwlon.qcompress.QCompress;

import java.io.IOException;
import java.util.function.Consumer;

public class QCompressLongWriter implements IntegerWriter {
  private static final int CHUNK_SIZE = 100000;
  private final PositionedOutputStream output;
  private final long[] bufferedLongs = new long[CHUNK_SIZE];
  private int longPos = 0;

  public QCompressLongWriter(PositionedOutputStream output) {
    this.output = output;
  }

  @Override
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
  }

  @Override
  public void write(long value) throws IOException {
    bufferedLongs[longPos] = value;
    if (longPos == CHUNK_SIZE - 1) {
      writeChunk();
    } else {
      longPos += 1;
    }
  }

  private void writeChunk() throws IOException {
    output.write(QCompress.autoCompressLongs(bufferedLongs, 6));
    longPos = 0;
  }

  @Override
  public void flush() throws IOException {
    if (longPos > 0) {
      writeChunk();
    }
    output.flush();
  }

  @Override
  public long estimateMemory() {
    return output.getBufferSize() + CHUNK_SIZE * 8;
  }

  @Override
  public void changeIv(Consumer<byte[]> modifier) {
    output.changeIv(modifier);
  }
}
