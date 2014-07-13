package tachyon.r;

import java.util.List;

public abstract class ShardBase {
  /**
   * Query the location of the key;
   * 
   * @param key
   * @return all nodes containing the key.
   */
  public abstract List<String> lookup(byte[] key);
}
