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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import java.nio.ByteBuffer;

/**
 * used in query.
 */
public class Chunk {

  private ChunkHeader chunkHeader;
  private ByteBuffer data;
  private TsFileInput fileInput;
  private long endPosition;
  /**
   * All data with timestamp <= deletedAt are considered deleted.
   */
  private long deletedAt;
  private EndianType endianType;

  public Chunk(ChunkHeader header, TsFileInput fileInput, long endPosition, long deletedAt, EndianType endianType) {
    this.chunkHeader = header;
    this.fileInput = fileInput;
    this.endPosition = endPosition;
    this.deletedAt = deletedAt;
    this.endianType = endianType;
  }

  public Chunk(ChunkHeader header, ByteBuffer data, long deletedAt, EndianType endianType) {
    this.chunkHeader = header;
    this.data = data;
    this.deletedAt = deletedAt;
    this.endianType = endianType;
  }

  public ChunkHeader getHeader() {
    return chunkHeader;
  }

  public TsFileInput getFileInput() {
    return fileInput;
  }

  public long getDeletedAt() {
    return deletedAt;
  }
  
  public EndianType getEndianType() {
	  return endianType;
  }

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }

  public ByteBuffer getData() {
    return data;
  }

  public long getEndPosition() {
    return endPosition;
  }
}
