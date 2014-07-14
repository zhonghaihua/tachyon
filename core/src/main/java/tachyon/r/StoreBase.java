package tachyon.r;

import tachyon.TachyonURI;

public abstract class StoreBase {
  protected final TachyonURI URI;

  public StoreBase(TachyonURI uri) {
    URI = uri;
  }

  public abstract byte[] get(byte[] key);

  public abstract void put(byte[] key, byte[] value);
}
