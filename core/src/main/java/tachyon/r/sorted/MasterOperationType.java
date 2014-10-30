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
    default :
      throw new IOException("Corrupted MasterOperationType.");   
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

  private MasterOperationType(int value) {
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
