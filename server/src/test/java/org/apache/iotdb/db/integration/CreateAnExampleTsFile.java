/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the IoTDB server should be
 * defined as integration test.
 */
public class CreateAnExampleTsFile {

  private static IoTDB daemon;

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;

  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();

    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.maxNumberOfPointsInPage;
    pageSizeInByte = tsFileConfig.pageSizeInByte;
    groupSizeInByte = tsFileConfig.groupSizeInByte;

    // new value
    tsFileConfig.maxNumberOfPointsInPage = 1000;
    tsFileConfig.pageSizeInByte = 1024 * 1024 * 150;
    tsFileConfig.groupSizeInByte = 1024 * 1024 * 1000;
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024 * 1024 * 1000);

    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();

    Thread.sleep(1000);
    insertData();

  }

  @AfterClass
  public static void tearDown() throws Exception {
    daemon.stop();
    // recovery value
    tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
    tsFileConfig.pageSizeInByte = pageSizeInByte;
    tsFileConfig.groupSizeInByte = groupSizeInByte;
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(groupSizeInByte);

    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    String[] create_sql = new String[]{"SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s5 WITH DATATYPE=DOUBLE, ENCODING=RLE",
    };
    try {
      connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();

      for (String sql : create_sql) {
        statement.execute(sql);
      }

      for (int time = 3000; time < 13600; time++) {
        String sql = String
                .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
        statement.execute(sql);
        sql = String
                .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql = String
                .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time,
                Constant.stringValue[time % 5]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time,
                Constant.booleanValue[time % 2]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
        statement.execute(sql);
      }

      for (int time = 13700; time < 24000; time++) {
        String sql = String
                .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.execute(sql);
        sql = String
                .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
        statement.execute(sql);
        sql = String
                .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        statement.execute(sql);
      }

      for (int time = 100000; time < 101000; time++) {
        String sql = String
                .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
        statement.execute(sql);
        sql = String
                .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
        statement.execute(sql);
        sql = String
                .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        statement.execute(sql);
      }
      statement.execute("flush");
      Thread.sleep(5000);
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void selectAllTest() throws IOException, StorageEngineException {
    String selectSql = "select * from root";
    //System.out.println("Test >>> " + selectSql);

    EngineQueryRouter engineExecutor = new EngineQueryRouter();
    QueryExpression queryExpression = QueryExpression.create();
    queryExpression.addSelectedPath(new Path(Constant.d0s0));
    queryExpression.addSelectedPath(new Path(Constant.d0s1));
    queryExpression.addSelectedPath(new Path(Constant.d0s2));
    queryExpression.addSelectedPath(new Path(Constant.d0s3));
    queryExpression.addSelectedPath(new Path(Constant.d0s4));
    queryExpression.addSelectedPath(new Path(Constant.d0s5));
    queryExpression.setExpression(null);

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
    QueryDataSet queryDataSet = engineExecutor.query(queryExpression, TEST_QUERY_CONTEXT);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      cnt++;
    }
    assertEquals(21900, cnt);

    QueryResourceManager.getInstance().endQueryForGivenJob(TEST_QUERY_JOB_ID);
  }

}