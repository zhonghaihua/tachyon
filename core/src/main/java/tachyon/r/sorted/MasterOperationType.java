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
 * Master operation types
 */
public enum MasterOperationType {
  CREATE_STORE(1), ADD_PARTITION(2), GET_PARTITION(3), NO_PARTITION(4);

  /**
   * Parse the MasterOperationType
   * 
   * @param op
   *          the ByteBuffer format of the write type
   * @return the MasterOperationType
   * @throws IOException
   */
  public static MasterOperationType getOpType(ByteBuffer op) throws IOException {
    if (op.limit() - op.position() != 4) {
      throw new IOException("Corrupted MasterOperationType.");
    }

    switch (op.getInt()) {
    case 1:
      return CREATE_STORE;
    case 2:
      return ADD_PARTITION;
    case 3:
      return GET_PARTITION;
    case 4:
      return NO_PARTITION;
    }

    throw new IOException("Corrupted MasterOperationType.");
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

  private MasterOperationType(int value) {
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
