/*
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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.ThreadPoolUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * TODO implement this class as TsFile DataSetWithoutTimeGenerator.
 */
public class EngineDataSetWithoutValueFilter extends QueryDataSet {

  private List<IPointReader> seriesReaderWithoutValueFilterList;

  private ConcurrentSkipListSet<Long> timeHeap;

  private List<ReaderCallable> callableList;

  /**
   * constructor of EngineDataSetWithoutValueFilter.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param readers readers in List(IPointReader) structure
   * @throws IOException IOException
   */
  public EngineDataSetWithoutValueFilter(List<Path> paths, List<TSDataType> dataTypes,
                                         List<IPointReader> readers)
      throws IOException {
    super(paths, dataTypes);
    this.seriesReaderWithoutValueFilterList = readers;
    this.callableList = new ArrayList<>(readers.size());
    initHeap();
  }

  private void initHeap() throws IOException {
    timeHeap = new ConcurrentSkipListSet<>();
    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      IPointReader reader = seriesReaderWithoutValueFilterList.get(i);
      this.callableList.add(new ReaderCallable(reader, dataTypes.get(i), 100));
    }
  }

  @Override
  public boolean hasNext() {
    return !timeHeap.isEmpty();
  }

  @Override
  public RowRecord next() throws IOException {
    long minTime = timeHeapGet();
    callableList.forEach(readerCallable -> readerCallable.setMinTime(minTime));
    RowRecord record = new RowRecord(minTime);
    ExecutorService executorService = ThreadPoolUtils.executorService;
    try {
      List<Future<Field>> results = executorService.invokeAll(callableList);
      for (Future<Field> fieldFuture : results) {
        Field field = fieldFuture.get();
        record.addField(field);
      }
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("MultiThread Wrong!!!!");
      e.printStackTrace();
    }
    return record;
  }

  private Field getField(TsPrimitiveType tsPrimitiveType, TSDataType dataType) {
    if (tsPrimitiveType == null) {
      return new Field(null);
    }
    Field field = new Field(dataType);
    switch (dataType) {
      case INT32:
        field.setIntV(tsPrimitiveType.getInt());
        break;
      case INT64:
        field.setLongV(tsPrimitiveType.getLong());
        break;
      case FLOAT:
        field.setFloatV(tsPrimitiveType.getFloat());
        break;
      case DOUBLE:
        field.setDoubleV(tsPrimitiveType.getDouble());
        break;
      case BOOLEAN:
        field.setBoolV(tsPrimitiveType.getBoolean());
        break;
      case TEXT:
        field.setBinaryV(tsPrimitiveType.getBinary());
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported: " + dataType);
    }
    return field;
  }

  /**
   * keep heap from storing duplicate time.
   */
  private void timeHeapPut(long time) {
    timeHeap.add(time);
  }

  private Long timeHeapGet() {
    return timeHeap.pollFirst();
  }

  public List<IPointReader> getReaders() {
    return seriesReaderWithoutValueFilterList;
  }

  private class ReaderCallable implements Callable<Field> {

    private IPointReader reader;

    private long minTime;

    private TSDataType dataType;

    private TimeValuePair[] cachedTimeValues;

    private int curIndex;

    private boolean needFetch;

    public ReaderCallable(IPointReader reader, TSDataType dataType, int cachedSize) throws IOException {
      this.reader = reader;
      this.dataType = dataType;
      this.cachedTimeValues = new TimeValuePair[cachedSize];
      this.curIndex = 0;
      if (reader.hasNext()) {
        TimeValuePair timeValuePair = reader.next();
        cachedTimeValues[0] = timeValuePair;
        timeHeapPut(timeValuePair.getTimestamp());
      }
      this.needFetch = true;
    }

    public void setMinTime(long minTime) {
      this.minTime = minTime;
    }

    @Override
    public Field call() throws Exception {
      TimeValuePair timeValuePair = cachedTimeValues[curIndex];
      if (timeValuePair == null) {
        return new Field(null);
      } else {
        if (timeValuePair.getTimestamp() == minTime) {
          curIndex++;
          if (curIndex == cachedTimeValues.length) {
            curIndex = 0;
            needFetch = true;
          }
          Field field =  getField(timeValuePair.getValue(), dataType);
          if (needFetch) {
            for (int i = curIndex; i < cachedTimeValues.length; i++) {
              if (reader.hasNext()) {
                cachedTimeValues[i] = reader.next();
                timeHeapPut(cachedTimeValues[i].getTimestamp());
              } else
                break;
            }
            needFetch = false;
          }
          return field;
        } else {
          return new Field(null);
        }
      }
    }
  }
}
