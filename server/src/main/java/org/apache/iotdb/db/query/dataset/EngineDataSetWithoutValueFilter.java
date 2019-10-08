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

import com.google.common.collect.Sets;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * TODO implement this class as TsFile DataSetWithoutTimeGenerator.
 */
public class EngineDataSetWithoutValueFilter extends QueryDataSet {

  private List<IPointReader> seriesReaderWithoutValueFilterList;

  private TimeValuePair[] cacheTimeValueList;

  private PriorityBlockingQueue<Long> timeHeap;

  private Set<Long> timeSet;

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
    initHeap();
  }

  private void initHeap() throws IOException {
    timeSet = Sets.newConcurrentHashSet();
    timeHeap = new PriorityBlockingQueue<>();
    cacheTimeValueList = new TimeValuePair[seriesReaderWithoutValueFilterList.size()];

    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      IPointReader reader = seriesReaderWithoutValueFilterList.get(i);
      if (reader.hasNext()) {
        TimeValuePair timeValuePair = reader.next();
        cacheTimeValueList[i] = timeValuePair;
        timeHeapPut(timeValuePair.getTimestamp());
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !timeHeap.isEmpty();
  }

  @Override
  public RowRecord next() throws IOException {
    long minTime = timeHeapGet();
    int fieldSize = seriesReaderWithoutValueFilterList.size();
    RowRecord record = new RowRecord(minTime, fieldSize);
    ExecutorService executorService = ThreadPoolUtils.executorService;
    Set<Callable<Boolean>> callableSet = new HashSet<>();
    for (int i = 0; i < fieldSize; i++) {
      final int index = i;
      callableSet.add(() -> {
        IPointReader reader = seriesReaderWithoutValueFilterList.get(index);
        if (cacheTimeValueList[index] == null) {
          record.putFieldByIndex(new Field(null), index);
        } else {
          if (cacheTimeValueList[index].getTimestamp() == minTime) {
            record.putFieldByIndex(getField(cacheTimeValueList[index].getValue(), dataTypes.get(index)), index);
            if (seriesReaderWithoutValueFilterList.get(index).hasNext()) {
              cacheTimeValueList[index] = reader.next();
              timeHeapPut(cacheTimeValueList[index].getTimestamp());
            }
          } else {
            record.putFieldByIndex(new Field(null), index);
          }
        }
        return true;
      });
    }
    try {
      executorService.invokeAll(callableSet);
    } catch (InterruptedException e) {
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
    if (!timeSet.contains(time)) {
      timeSet.add(time);
      timeHeap.add(time);
    }
  }

  private Long timeHeapGet() {
    Long t = timeHeap.poll();
    timeSet.remove(t);
    return t;
  }

  public List<IPointReader> getReaders() {
    return seriesReaderWithoutValueFilterList;
  }
}
