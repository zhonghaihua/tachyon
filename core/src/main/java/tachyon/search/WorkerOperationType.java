package tachyon.search;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Worker operation types
 */
public enum WorkerOperationType {
  QUERY(1);

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
      return QUERY;
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
