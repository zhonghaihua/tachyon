package tachyon.r;

public abstract class StoreBase {
  protected final String URI;

  public StoreBase(String uri) {
    URI = uri;
  }

  public abstract byte[] get(byte[] key);

  public abstract void put(byte[] key, byte[] value);
}
