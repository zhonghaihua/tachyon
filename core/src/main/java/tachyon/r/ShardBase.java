package tachyon.r;

import java.util.List;

public interface ShardBase {
  /**
   * Query the location of the key;
   * 
   * @param key
   * @return all partitions (ids) containing the key;
   */
  public abstract List<Integer> lookup(byte[] key);
}
