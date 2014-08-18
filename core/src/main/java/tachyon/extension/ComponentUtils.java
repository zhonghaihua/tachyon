package tachyon.extension;

import java.nio.ByteBuffer;
import java.util.List;

public class ComponentUtils {
  protected void lengthCheck(List<ByteBuffer> data, int length, String opType)
      throws ComponentException {
    if (data.size() != length) {
      throw new ComponentException("Corrupted " + opType + " data, wrong data length "
          + data.size() + " . Right length is " + length);
    }
  }

  protected void minLengthCheck(List<ByteBuffer> data) throws ComponentException {
    if (data.size() < 1) {
      throw new ComponentException("Data List is empty");
    }
  }
}
