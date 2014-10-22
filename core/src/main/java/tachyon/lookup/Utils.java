package tachyon.lookup;

public final class Utils {
  public static int hash(byte[] key) {
    if (key == null || key.length == 0) {
      return 0;
    }
    int ret = 13;
    for (byte value : key) {
      ret = (ret * value) % 9997;
    }
    return ret;
  }
}
