package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Yuan Tian
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PageReaderTest {
  private ReadOnlyTsFile readTsFile;
  private ArrayList<Path> paths = new ArrayList<>();
  private IExpression timeFilter;

//  @Param({"45", "1000", "10000", "100000", "500000"})
//  private int bufferCapacity; // ensure that the file to read is large enough
//  @Param({"99", "10000", "100000","300000", "1000000", "1500000", "2500000", "3000000", "3200000"})
//  private long pos; // ensure that the file to read is large enough


  @Setup(Level.Invocation) // please make sure you understand Level.Invocation
  public void prepare() throws IOException {
    String file = "/Users/jackietien/Desktop/IoTDB/查询/1574251382232-101-0.tsfile";
    TsFileSequenceReader reader = new TsFileSequenceReader(file);
    readTsFile = new ReadOnlyTsFile(reader);
    paths.add(new Path("root.group_0.d_0.s_3"));
    timeFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1605482800000L)),
            new GlobalTimeExpression(TimeFilter.ltEq(1605482850000L)));
  }

  @TearDown(Level.Invocation)
  public void close() throws IOException {
    readTsFile.close();
  }

  @Benchmark
  public List<RowRecord> test() throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    List<RowRecord> res = new ArrayList<>();
    while (queryDataSet.hasNext()) {
      res.add(queryDataSet.next());
    }
    return res;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(PageReaderTest.class.getSimpleName())
            .forks(1)
            .warmupIterations(10)
            .measurementIterations(10)
            .build();

    new Runner(opt).run();
  }
}