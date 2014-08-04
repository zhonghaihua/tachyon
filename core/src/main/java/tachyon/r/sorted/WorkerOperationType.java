/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.r.sorted;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Worker operation types
 */
public enum WorkerOperationType {
  GET(1);

  /**
   * Parse the WorkerOperationType
   * 
   * @param op
   *          the ByteBuffer format of the write type
   * @return the WorkerOperationType
   * @throws IOException
   */
  public static WorkerOperationType getOpType(ByteBuffer op) throws IOException {
    if (op.limit() - op.position() != 4) {
      throw new IOException("Corrupted MasterOperationType.");
    }

    switch (op.getInt()) {
    case 1:
      return GET;
    }

    throw new IOException("Corrupted WorkerOperationType.");
  }

  /**
   * @return the ByteBuffer representation of the MasterOperationType.
   */
  public ByteBuffer toByteBuffer() {
    ByteBuffer res = ByteBuffer.allocate(4);
    res.putInt(VALUE);
    res.flip();
    return res;
  }

  private final int VALUE;

  private WorkerOperationType(int value) {
    VALUE = value;
  }

  /**
   * Return the value of the write type
   * 
   * @return the value of the write type
   */
  public int getValue() {
    return VALUE;
  }
}
