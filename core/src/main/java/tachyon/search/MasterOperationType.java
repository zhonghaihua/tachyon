package tachyon.search;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Master operation types
 */
public enum MasterOperationType {
  CREATE_STORE(1), ADD_SHARD(2), GET_WORKERS(3);

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
      return ADD_SHARD;
    case 3:
      return GET_WORKERS;
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
