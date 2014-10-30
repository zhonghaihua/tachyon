package tachyon.r;

public abstract class MasterStoreBase {
  protected final String mURI;

  public MasterStoreBase(String uri) {
    mURI = uri;
  }
  
  public abstract MasterStoreBase createStore();
  
}
