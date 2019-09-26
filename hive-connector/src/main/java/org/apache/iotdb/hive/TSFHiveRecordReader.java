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
package org.apache.iotdb.hive;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.HDFSInput;
import org.apache.iotdb.tsfile.hadoop.TSFInputFormat;
import org.apache.iotdb.tsfile.hadoop.TSFInputSplit;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author Yuan Tian
 */
public class TSFHiveRecordReader implements RecordReader<NullWritable, MapWritable>, JobConfigurable {

  private static final Logger logger = LoggerFactory.getLogger(TSFHiveRecordReader.class);

  /**
   * all
   */
  private List<QueryDataSet> dataSetList = new ArrayList<>();
  /**
   * List for name of devices. The order corresponds to the order of dataSetList.
   * Means that deviceIdList[i] is the name of device for dataSetList[i].
   */
  private List<String> deviceIdList = new ArrayList<>();
  private List<Field> fields = null;
  /**
   * The index of QueryDataSet that is currently processed
   */
  private  int currentIndex = 0;
  private long timestamp = 0;
  private boolean isReadDeviceId;
  private boolean isReadTime;
  private int arraySize = 0;
  private TsFileSequenceReader reader;
  private List<String> measurementIds;
  private JobConf jobConf;

  @Override
  public boolean next(NullWritable key, MapWritable value) throws IOException {
    while (currentIndex < dataSetList.size()) {
      if (!dataSetList.get(currentIndex).hasNext()) {
        currentIndex++;
      }
      else {
        RowRecord rowRecord = dataSetList.get(currentIndex).next();
        fields = rowRecord.getFields();
        timestamp = rowRecord.getTimestamp();

        try {
          value.putAll(getCurrentValue());
        } catch (InterruptedException e) {
          throw new IOException(e.getMessage());
        }

        return true;
      }
    }
    return false;
  }

  @Override
  public NullWritable createKey() {
    return  NullWritable.get();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    // can't know
    return 0;
  }


  public TSFHiveRecordReader(InputSplit split, JobConf job)
          throws IOException {
    if (split instanceof TSFInputSplit) {
      TSFInputSplit tsfInputSplit = (TSFInputSplit) split;
      org.apache.hadoop.fs.Path path = tsfInputSplit.getPath();
      List<TSFInputSplit.ChunkGroupInfo> chunkGroupInfoList = tsfInputSplit.getChunkGroupInfoList();
      reader = new TsFileSequenceReader(new HDFSInput(path, job));

      // Get the read columns and filter information
      List<String> deltaObjectIds = TSFInputFormat.getReadDeltaObjectIds(job);
      if (deltaObjectIds == null) {
        deltaObjectIds = initDeviceIdList(chunkGroupInfoList);
      }
      List<String> measurementIds = TSFInputFormat.getReadMeasurementIds(job);
      if (measurementIds == null) {
        measurementIds = initSensorIdList(chunkGroupInfoList);
      }
      this.measurementIds = measurementIds;
      logger.info("deltaObjectIds:" + deltaObjectIds);
      logger.info("Sensors:" + measurementIds);

      isReadDeviceId = TSFInputFormat.getReadDeltaObject(job);
      isReadTime = TSFInputFormat.getReadTime(job);
      if (isReadDeviceId) {
        arraySize++;
      }
      if (isReadTime) {
        arraySize++;
      }
      arraySize += measurementIds.size();
      ReadOnlyTsFile queryEngine = new ReadOnlyTsFile(reader);
      for (TSFInputSplit.ChunkGroupInfo chunkGroupInfo : chunkGroupInfoList) {
        String deviceId = chunkGroupInfo.getDeviceId();
        if (deltaObjectIds.contains(deviceId)) {
          List<Path> paths = measurementIds.stream()
                  .map(measurementId -> new Path(deviceId + TsFileConstant.PATH_SEPARATOR + measurementId))
                  .collect(toList());
          QueryExpression queryExpression = QueryExpression.create(paths, null);
          QueryDataSet dataSet = queryEngine.query(queryExpression,
                  chunkGroupInfo.getStartOffset(), chunkGroupInfo.getEndOffset());
          dataSetList.add(dataSet);
          deviceIdList.add(deviceId);
        }
      }

    } else {
      logger.error("The InputSplit class is not {}, the class is {}", TSFInputSplit.class.getName(),
              split.getClass().getName());
      throw new InternalError(String.format("The InputSplit class is not %s, the class is %s",
              TSFInputSplit.class.getName(), split.getClass().getName()));
    }
  }

  private List<String> initDeviceIdList(List<TSFInputSplit.ChunkGroupInfo> chunkGroupInfoList) {
    return chunkGroupInfoList.stream()
            .map(TSFInputSplit.ChunkGroupInfo::getDeviceId)
            .distinct()
            .collect(toList());
  }

  private List<String> initSensorIdList(List<TSFInputSplit.ChunkGroupInfo> chunkGroupInfoList) {
    return chunkGroupInfoList.stream()
            .flatMap(chunkGroupMetaData -> Arrays.stream(chunkGroupMetaData.getMeasurementIds()))
            .distinct()
            .collect(toList());
  }

  private MapWritable getCurrentValue() throws InterruptedException {
    MapWritable mapWritable = new MapWritable();
    Text deviceIdText = new Text(deviceIdList.get(currentIndex));
    LongWritable time = new LongWritable(timestamp);

    if (isReadTime) { // Only Time needs to be written into value
      mapWritable.put(new Text("time_stamp"), time);
    }
    if (isReadDeviceId) { // Only deviceId need to be written into value
      mapWritable.put(new Text("device_id"), deviceIdText);
    }

    readFieldsValue(mapWritable);

    return mapWritable;
  }

  /**
   * Read from current fields value
   * @param mapWritable where to write
   * @throws InterruptedException
   */
  private void readFieldsValue(MapWritable mapWritable) throws InterruptedException {
    int index = 0;
    for (Field field : fields) {
      if (field.isNull()) {
        logger.info("Current value is null");
        mapWritable.put(new Text(measurementIds.get(index)), NullWritable.get());
      } else {
        switch (field.getDataType()) {
          case INT32:
            mapWritable.put(new Text(measurementIds.get(index)), new IntWritable(field.getIntV()));
            break;
          case INT64:
            mapWritable.put(new Text(measurementIds.get(index)), new LongWritable(field.getLongV()));
            break;
          case FLOAT:
            mapWritable.put(new Text(measurementIds.get(index)), new FloatWritable(field.getFloatV()));
            break;
          case DOUBLE:
            mapWritable.put(new Text(measurementIds.get(index)), new DoubleWritable(field.getDoubleV()));
            break;
          case BOOLEAN:
            mapWritable.put(new Text(measurementIds.get(index)), new BooleanWritable(field.getBoolV()));
            break;
          case TEXT:
            mapWritable.put(new Text(measurementIds.get(index)), new Text(field.getBinaryV().getStringValue()));
            break;
          default:
            logger.error("The data type is not support {}", field.getDataType());
            throw new InterruptedException(
                    String.format("The data type %s is not support ", field.getDataType()));
        }
      }
      index++;
    }
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    dataSetList = null;
    deviceIdList = null;
    reader.close();
  }

  @Override
  public void configure(JobConf job) {
    this.jobConf = job;
  }
}