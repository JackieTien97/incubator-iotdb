package org.apache.iotdb.db.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolUtils {
  public static final ExecutorService executorService = Executors.newCachedThreadPool();

}
