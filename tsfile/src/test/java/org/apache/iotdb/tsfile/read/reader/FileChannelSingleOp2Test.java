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
public class FileChannelSingleOp2Test {
  // total size 3729209
  private final Path file = Paths.get("test.tsfile");

  @Param({"99", "10000", "100000", "300000", "1000000", "1500000", "2500000", "3000000", "3200000"})
  private long pos; // ensure that the file to read is large enough
  @Param({"12", "9999", "99999","299999", "1499999", "2499999", "3200000", "3700000"})
  private long initialPos;

  private FileChannel channel;

  @Setup(Level.Invocation) // please make sure you understand Level.Invocation
  public void prepare() throws IOException {
    channel = FileChannel.open(file, StandardOpenOption.READ);
    channel.position(initialPos);
  }

  @TearDown(Level.Invocation) // please make sure you understand Level.Invocation
  public void close() throws IOException {
    if (channel != null) {
      channel.close();
    }
  }


  @Benchmark
  public FileChannel test() throws IOException {
    channel.position(pos);
    return channel;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(FileChannelSingleOp2Test.class.getSimpleName())
            .forks(1)
            .warmupIterations(10)
            .measurementIterations(10)
            .build();

    new Runner(opt).run();
  }
}
