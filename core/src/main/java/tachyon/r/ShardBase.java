package tachyon.r;

import java.util.List;

public abstract class ShardBase {
  private final boolean IS_MASTER;

  protected ShardBase(boolean isMaster) {
    IS_MASTER = isMaster;
  }

  protected boolean isMaster() {
    return IS_MASTER;
  }

  /**
   * Query the location of the key;
   * 
   * @param key
   * @return all nodes containing the key.
   */
  public abstract List<String> lookup(byte[] key);
}
