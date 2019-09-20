/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read.reader;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

/**
 * @author Yuan Tian
 */

/**
 * my test.tsfile 2019/09/18 22:40
 * Benchmark                       Mode  Cnt   Score   Error  Units
 * FileChannelMultiOpTest.test1_1  avgt   20  10.886 ± 0.041  us/op
 * FileChannelMultiOpTest.test1_2  avgt   20  16.417 ± 0.161  us/op
 * FileChannelMultiOpTest.test1_3  avgt   20  13.645 ± 0.041  us/op
 *
 * my test.tsfile 2019/09/19 16:50
 * Benchmark                       Mode  Cnt   Score   Error  Units
 * FileChannelMultiOpTest.test1_1  avgt   20  11.324 ± 0.383  us/op
 * FileChannelMultiOpTest.test1_2  avgt   20  18.219 ± 1.307  us/op
 * FileChannelMultiOpTest.test1_3  avgt   20  13.603 ± 0.035  us/op
 *
 * Lei's test.file 2019/09/19 17:02
 * Benchmark                       Mode  Cnt   Score   Error  Units
 * FileChannelMultiOpTest.test1_1  avgt   20  11.206 ± 0.291  us/op
 * FileChannelMultiOpTest.test1_2  avgt   20  16.837 ± 0.424  us/op
 * FileChannelMultiOpTest.test1_3  avgt   20  14.408 ± 0.393  us/op
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FileChannelMultiOpTest {
  // total size 3729209
  private final Path file = Paths.get("/Users/jackietien/Documents/test.tsfile");
  private FileChannel channel1; // 在初始位置12处执行read(buffer, pos)函数

  private FileChannel channel2; // 在初始位置12处执行position(pos)函数

  private FileChannel channel3; // 在pos位置处执行read(buffer)函数

  private int[] position;
  private ByteBuffer[] buffers1;
  private ByteBuffer[] buffers2;
  private ByteBuffer[] buffers3;

  @Setup(Level.Invocation) // please make sure you understand Level.Invocation
  public void prepare() throws IOException {
    position = new int[] {3634, 3638, 3662, 4589, 4593, 4617, 13, 17, 41, 5939, 5939};
    int[] bufferSize = {4, 24, 926, 4, 24, 1289, 4, 24, 2653, 623, 623};
    buffers1 = new ByteBuffer[bufferSize.length];
    buffers2 = new ByteBuffer[bufferSize.length];
    buffers3 = new ByteBuffer[bufferSize.length];
    for (int i = 0; i < bufferSize.length; i++) {
      buffers1[i] = ByteBuffer.allocate(bufferSize[i]);
      buffers2[i] = ByteBuffer.allocate(bufferSize[i]);
      buffers3[i] = ByteBuffer.allocate(bufferSize[i]);
    }
    channel1 = FileChannel.open(file, StandardOpenOption.READ);
    channel1.position(12);

    channel2 = FileChannel.open(file, StandardOpenOption.READ);
    channel2.position(12);

    channel3 = FileChannel.open(file, StandardOpenOption.READ);
    channel3.position(12);
  }

  @TearDown(Level.Invocation) // please make sure you understand Level.Invocation
  public void close() throws IOException {
    if (channel1 != null) {
      channel1.close();
    }
    if (channel2 != null) {
      channel2.close();
    }
    if (channel3 != null) {
      channel3.close();
    }
  }

  @Benchmark
  public ByteBuffer[] test1_1() throws IOException {
    for (int i = 0; i < buffers1.length; i++) {
      channel1.read(buffers1[i], position[i]);
    }
    return buffers1;
  }

  @Benchmark
  public ByteBuffer[] test1_2() throws IOException {
    for (int i = 0; i < buffers2.length; i++) {
      channel2.position(position[i]);
      channel2.read(buffers2[i]);
    }
    return buffers2;
  }

  @Benchmark
  public ByteBuffer[] test1_3() throws IOException {
    channel3.position(position[0]);
    channel3.read(buffers3[0]);
    channel3.read(buffers3[1]);
    channel3.read(buffers3[2]);
    channel3.position(position[3]);
    channel3.read(buffers3[3]);
    channel3.read(buffers3[4]);
    channel3.read(buffers3[5]);
    channel3.position(position[6]);
    channel3.read(buffers3[6]);
    channel3.read(buffers3[7]);
    channel3.read(buffers3[8]);
    channel3.position(position[9]);
    channel3.read(buffers3[9]);
    channel3.position(position[10]);
    channel3.read(buffers3[10]);
    return buffers3;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(FileChannelMultiOpTest.class.getSimpleName())
            .forks(1)
            .warmupIterations(10)
            .measurementIterations(20)
            .build();

    new Runner(opt).run();
  }
}
