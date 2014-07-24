package tachyon.r;

public abstract class MasterStoreBase {
  protected final String URI;

  public MasterStoreBase(String uri) {
    URI = uri;
  }
  
  public abstract MasterStoreBase createStore();
  
}
