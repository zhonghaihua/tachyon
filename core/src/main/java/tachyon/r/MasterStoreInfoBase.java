package tachyon.r;

public abstract class MasterStoreInfoBase {
  protected final String URI;

  public MasterStoreInfoBase(String uri) {
    URI = uri;
  }
  
  public abstract MasterStoreInfoBase createStore();
  
}
