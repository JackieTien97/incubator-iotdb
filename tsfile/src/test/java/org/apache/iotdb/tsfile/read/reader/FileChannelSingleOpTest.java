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
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FileChannelSingleOpTest {
  // total size 3729209
  private final Path file = Paths.get("test.tsfile");

  @Param({"45", "1000", "10000", "100000", "500000"})
  private int bufferCapacity; // ensure that the file to read is large enough
  @Param({"99", "10000", "100000","300000", "1000000", "1500000", "2500000", "3000000", "3200000"})
  private long pos; // ensure that the file to read is large enough

  private FileChannel channel1; // 在初始位置12处执行read(buffer, pos)函数
  private ByteBuffer buffer1;

  private FileChannel channel2; // 在初始位置12处执行position(pos)函数

  private FileChannel channel3; // 在pos位置处执行read(buffer)函数
  private ByteBuffer buffer3;

  @Setup(Level.Invocation) // please make sure you understand Level.Invocation
  public void prepare() throws IOException {
    channel1 = FileChannel.open(file, StandardOpenOption.READ);
    channel1.position(12);
    buffer1 = ByteBuffer.allocate(bufferCapacity);

    channel2 = FileChannel.open(file, StandardOpenOption.READ);
    channel2.position(12);

    channel3 = FileChannel.open(file, StandardOpenOption.READ);
    channel3.position(pos);
    buffer3 = ByteBuffer.allocate(bufferCapacity);
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
  public ByteBuffer test1_1() throws IOException {
    channel1.read(buffer1, pos);
    return buffer1;
  }

  @Benchmark
  public FileChannel test1_2() throws IOException {
    channel2.position(pos);
    return channel2;
  }

  @Benchmark
  public ByteBuffer test1_3() throws IOException {
    channel3.read(buffer3);
    return buffer3;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(FileChannelSingleOpTest.class.getSimpleName())
            .forks(1)
            .warmupIterations(10)
            .measurementIterations(10)
            .build();

    new Runner(opt).run();
  }
}
