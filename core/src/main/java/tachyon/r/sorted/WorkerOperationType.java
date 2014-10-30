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
    default:
      throw new IOException("Corrupted WorkerOperationType.");
    }
  }

  /**
   * @return the ByteBuffer representation of the MasterOperationType.
   */
  public ByteBuffer toByteBuffer() {
    ByteBuffer res = ByteBuffer.allocate(4);
    res.putInt(mValue);
    res.flip();
    return res;
  }

  private final int mValue;

  private WorkerOperationType(int value) {
    mValue = value;
  }

  /**
   * Return the value of the write type
   * 
   * @return the value of the write type
   */
  public int getValue() {
    return mValue;
  }
}
