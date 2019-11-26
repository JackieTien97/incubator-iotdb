package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Yuan Tian
 */
public class PageReaderTest {

  public static void main(String[] args) throws IOException {
    long[] times = new long[15];
    for (int i = 1; i <= 15; i++) {
      String file = "/Users/jackietien/Desktop/IoTDB/1574251382232-101-0.tsfile";
      TsFileSequenceReader reader = null;
      QueryDataSet queryDataSet;
      reader = new TsFileSequenceReader(file);
      ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("root.group_0.d_0.s_3"));
      IExpression chunkFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(1604482800000L)),
              new GlobalTimeExpression(TimeFilter.ltEq(1671592795000L)));
      QueryExpression queryExpression = QueryExpression.create(paths, chunkFilter);
      long startTime = System.currentTimeMillis();
      queryDataSet = readTsFile.query(queryExpression);
      List<RowRecord> res = new LinkedList<>();
      while (queryDataSet.hasNext()) {
        res.add(queryDataSet.next());
      }
      long endTime = System.currentTimeMillis();
      times[i-1] = endTime - startTime;
      System.out.println(res.size());
      reader.close();
      queryDataSet.close();
    }
    long res = 0;
    for (int i = 0; i < times.length; i++) {
      System.out.println(times[i]);
      if (i > 4)
        res += times[i];
    }
    System.out.println("average time: " + (res / 10.0));
  }

}