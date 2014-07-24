package tachyon.r;

import java.io.IOException;

import tachyon.TachyonURI;

public abstract class StoreBase {
  protected final TachyonURI URI;

  public StoreBase(TachyonURI uri) {
    URI = uri;
  }

  public abstract byte[] get(byte[] key) throws IOException;

  public abstract void put(byte[] key, byte[] value) throws IOException;
}
